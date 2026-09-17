package condition

import (
	"fmt"
	"maps"

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

	inferredIdents := extractIdents(ast.NativeRep().Expr(), nil)
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

// extractIdents recursively walks a CEL expression and returns the names of all
// variable identifiers, excluding those bound by enclosing comprehensions (locals).
func extractIdents(expr celast.Expr, locals map[string]bool) []string {
	if expr == nil {
		return nil
	}
	switch expr.Kind() {
	case celast.IdentKind:
		name := expr.AsIdent()
		if locals[name] {
			return nil
		}
		return []string{name}

	case celast.SelectKind:
		return extractIdents(expr.AsSelect().Operand(), locals)

	case celast.CallKind:
		call := expr.AsCall()
		var result []string
		if call.IsMemberFunction() {
			result = append(result, extractIdents(call.Target(), locals)...)
		}
		for _, arg := range call.Args() {
			result = append(result, extractIdents(arg, locals)...)
		}
		return result

	case celast.ListKind:
		var result []string
		for _, elem := range expr.AsList().Elements() {
			result = append(result, extractIdents(elem, locals)...)
		}
		return result

	case celast.MapKind:
		var result []string
		for _, entry := range expr.AsMap().Entries() {
			me := entry.AsMapEntry()
			result = append(result, extractIdents(me.Key(), locals)...)
			result = append(result, extractIdents(me.Value(), locals)...)
		}
		return result

	case celast.StructKind:
		var result []string
		for _, field := range expr.AsStruct().Fields() {
			result = append(result, extractIdents(field.AsStructField().Value(), locals)...)
		}
		return result

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

		var result []string
		result = append(result, extractIdents(comp.IterRange(), locals)...)
		result = append(result, extractIdents(comp.AccuInit(), innerLocals)...)
		result = append(result, extractIdents(comp.LoopCondition(), innerLocals)...)
		result = append(result, extractIdents(comp.LoopStep(), innerLocals)...)
		result = append(result, extractIdents(comp.Result(), innerLocals)...)
		return result
	}
	return nil
}
