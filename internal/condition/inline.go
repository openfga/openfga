package condition

import (
	"fmt"
	"maps"
	"slices"

	celast "github.com/google/cel-go/common/ast"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/condition/types"
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

// NewCompiledFromInlineExpression builds a fully compiled EvaluableCondition from
// the context Struct attached to a $expression tuple condition. The Struct must
// contain an "expression" string key (required) and may contain a "parameters"
// nested Struct whose values are type-name strings (e.g. "string", "int").
//
// Any identifiers used in the expression that are not declared in "parameters" are
// automatically inferred as type string. Comprehension-scoped variables are excluded.
//
// The reserved keys "expression" and "parameters" cannot be used as parameter names.
func NewCompiledFromInlineExpression(ctx *structpb.Struct) (*EvaluableCondition, error) {
	fields := ctx.GetFields()

	// 1. Extract the CEL expression string (required).
	exprVal, ok := fields[inlineContextExpressionKey]
	if !ok {
		return nil, fmt.Errorf("%s: missing required context field %q", InlineExpressionName, inlineContextExpressionKey)
	}
	exprStr := exprVal.GetStringValue()
	if exprStr == "" {
		return nil, fmt.Errorf("%s: context field %q must be a non-empty string", InlineExpressionName, inlineContextExpressionKey)
	}

	// 2. Extract explicitly declared parameter types (optional).
	declaredParams, err := extractDeclaredParameters(fields)
	if err != nil {
		return nil, err
	}

	// 3. Parse the CEL expression and infer undeclared identifiers as string.
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

	// 4. Build and fully compile the condition.
	cond := &openfgav1.Condition{
		Name:       InlineExpressionName,
		Expression: exprStr,
		Parameters: declaredParams,
	}
	return NewCompiled(cond)
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
