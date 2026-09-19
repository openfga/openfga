package condition

import (
	"context"
	"fmt"
	"maps"
	"slices"

	celast "github.com/google/cel-go/common/ast"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/condition/types"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/tuple"
)

// InlineExpressionName is the reserved condition name used for $expression inline conditions.
const InlineExpressionName = "$expression"

// inlineContextExpressionKey and inlineContextParametersKey are the reserved keys
// in the $expression condition context Struct.
const (
	inlineContextExpressionKey = "expression"
	inlineContextParametersKey = "parameters"
)

// IsInlineExpression reports whether a condition name is the $expression reserved symbol.
func IsInlineExpression(name string) bool {
	return name == InlineExpressionName
}

type conditionParameters = map[string]*openfgav1.ConditionParamTypeRef

func extractDeclaredParameters(fields map[string]*structpb.Value) (conditionParameters, error) {
	var declaredParams conditionParameters

	if paramVal, ok := fields[inlineContextParametersKey]; ok {
		ps := paramVal.GetStructValue()
		if ps == nil {
			return nil, fmt.Errorf("%s: context field %q must be a struct", InlineExpressionName, inlineContextParametersKey)
		}
		for paramName, typeVal := range ps.GetFields() {
			if paramName == inlineContextExpressionKey || paramName == inlineContextParametersKey {
				return nil, fmt.Errorf(
					"%s: parameter name %q is reserved and cannot be used", InlineExpressionName, paramName,
				)
			}
			typeName, knownType := types.TypeNameFromString(typeVal.GetStringValue())
			if !knownType {
				return nil, fmt.Errorf(
					"%s: unknown parameter type %q for parameter %q", InlineExpressionName, typeVal.GetStringValue(), paramName,
				)
			}
			if declaredParams == nil {
				declaredParams = make(conditionParameters)
			}
			declaredParams[paramName] = &openfgav1.ConditionParamTypeRef{TypeName: typeName}
		}
	}
	return declaredParams, nil
}

// FromInlineExpression parses, compiles, and returns an EvaluableCondition for the
// $expression inline condition carried by tk. When the request context contains an
// inlineConditionCache (injected by NewContextWithInlineConditionCache), the compiled
// result is stored and reused across all calls within the same request, eliminating
// redundant CEL compilation for repeated tuple evaluations.
func FromInlineExpression(ctx context.Context, tk *openfgav1.TupleKey) (*EvaluableCondition, error) {
	c, hasCache := ctx.Value(inlineConditionCacheCtxKey{}).(*inlineConditionCache)
	if hasCache {
		cacheKey := tuple.TupleKeyToString(tk)

		c.mu.RLock()
		ec, hit := c.m[cacheKey]
		c.mu.RUnlock()
		if hit {
			return ec, nil
		}

		v, err, _ := c.sf.Do(cacheKey, func() (any, error) {
			ec, err := compileInlineExpression(tk.GetCondition().GetContext())
			if err != nil {
				return nil, err
			}
			c.mu.Lock()
			c.m[cacheKey] = ec
			c.mu.Unlock()
			return ec, nil
		})
		if err != nil {
			return nil, err
		}
		return v.(*EvaluableCondition), nil
	}

	return compileInlineExpression(tk.GetCondition().GetContext())
}

// compileInlineExpression parses the $expression struct, infers parameter types,
// creates an EvaluableCondition, and compiles the CEL program before returning.
func compileInlineExpression(structCtx *structpb.Struct) (*EvaluableCondition, error) {
	fields := structCtx.GetFields()

	exprVal, ok := fields[inlineContextExpressionKey]
	if !ok {
		return nil, fmt.Errorf("%s: missing required context field %q", InlineExpressionName, inlineContextExpressionKey)
	}

	exprStr := exprVal.GetStringValue()
	if exprStr == "" {
		return nil, fmt.Errorf("%s: context field %q must be a non-empty string", InlineExpressionName, inlineContextExpressionKey)
	}

	declaredParams, err := extractDeclaredParameters(fields)
	if err != nil {
		return nil, err
	}

	ast, issues := celBaseEnv.Parse(exprStr)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("%s: invalid CEL expression: %w", InlineExpressionName, issues.Err())
	}

	inferredIdents := extractIdents(ast.NativeRep().Expr())

	if declaredParams == nil && len(inferredIdents) > 0 {
		declaredParams = make(conditionParameters, len(inferredIdents))
	}

	for _, ident := range inferredIdents {
		if _, alreadyDeclared := declaredParams[ident]; !alreadyDeclared {
			declaredParams[ident] = &openfgav1.ConditionParamTypeRef{
				TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
			}
		}
	}

	ec := NewUncompiled(&openfgav1.Condition{
		Name:       InlineExpressionName,
		Expression: exprStr,
		Parameters: declaredParams,
	}).
		WithTrackEvaluationCost().
		WithMaxEvaluationCost(config.MaxConditionEvaluationCost()).
		WithInterruptCheckFrequency(config.DefaultInterruptCheckFrequency)

	if err := ec.Compile(); err != nil {
		return nil, err
	}
	return ec, nil
}

// extractIdents iteratively walks a CEL expression and returns the names of all
// variable identifiers, excluding those bound by enclosing comprehensions (locals).
func extractIdents(expr celast.Expr) []string {
	if expr == nil {
		return nil
	}

	type stackItem struct {
		expr   celast.Expr
		locals map[string]bool
	}

	var results []string

	stack := []stackItem{{expr: expr, locals: nil}}

	for len(stack) > 0 {
		idx := len(stack) - 1
		current := stack[idx]
		stack = stack[:idx]

		expr := current.expr
		locals := current.locals

		switch expr.Kind() {
		case celast.IdentKind:
			name := expr.AsIdent()
			if locals[name] {
				continue
			}
			results = append(results, name)

		case celast.SelectKind:
			stack = append(stack, stackItem{expr: expr.AsSelect().Operand(), locals: locals})

		case celast.CallKind:
			call := expr.AsCall()

			for _, arg := range slices.Backward(call.Args()) {
				stack = append(stack, stackItem{expr: arg, locals: locals})
			}

			if call.IsMemberFunction() {
				stack = append(stack, stackItem{expr: call.Target(), locals: locals})
			}

		case celast.ListKind:
			for _, elem := range slices.Backward(expr.AsList().Elements()) {
				stack = append(stack, stackItem{expr: elem, locals: locals})
			}

		case celast.MapKind:
			for _, entry := range slices.Backward(expr.AsMap().Entries()) {
				me := entry.AsMapEntry()
				stack = append(stack, stackItem{expr: me.Key(), locals: locals})
				stack = append(stack, stackItem{expr: me.Value(), locals: locals})
			}

		case celast.StructKind:
			for _, field := range slices.Backward(expr.AsStruct().Fields()) {
				stack = append(stack, stackItem{expr: field.AsStructField().Value(), locals: locals})
			}

		case celast.ComprehensionKind:
			comp := expr.AsComprehension()
			// Create a new locals scope that includes the comprehension-bound variables.
			innerLocals := maps.Clone(locals)
			if innerLocals == nil {
				innerLocals = map[string]bool{}
			}
			innerLocals[comp.IterVar()] = true
			if comp.HasIterVar2() {
				innerLocals[comp.IterVar2()] = true
			}
			innerLocals[comp.AccuVar()] = true

			stack = append(stack, stackItem{expr: comp.Result(), locals: innerLocals})
			stack = append(stack, stackItem{expr: comp.LoopStep(), locals: innerLocals})
			stack = append(stack, stackItem{expr: comp.LoopCondition(), locals: innerLocals})
			stack = append(stack, stackItem{expr: comp.AccuInit(), locals: innerLocals})
			stack = append(stack, stackItem{expr: comp.IterRange(), locals: locals})
		}
	}
	return results
}
