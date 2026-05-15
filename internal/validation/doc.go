// Package validation provides tuple and request validation against an
// OpenFGA authorization model.
//
// The primary entry points are [ValidateTupleForWrite] (used by write
// paths and contextual tuples) and [ValidateTupleForRead] (used by
// read/query paths). Both enforce type restrictions, tupleset
// constraints, and condition requirements defined in the model.
//
// A generic [Validator] type and combinators ([CombineValidators],
// [MakeFallible]) support composing reusable validation predicates
// outside of the tuple-specific logic.
package validation
