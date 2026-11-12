package conditions

// Gap is a known difference between what the server does today and the agreed
// target behavior. Each gap is closed by one proposed improvement.
//
// A cell tied to a gap passes only while the server still gives the result
// recorded as its Today value. When the improvement lands and the cell starts
// returning its Want value, the run fails with a GAP CLOSED message, and the
// gap is removed from the generator that set it. The list can only shrink.
type Gap struct {
	// Improvement is the number of the proposed improvement that closes the gap.
	Improvement int
	// Summary says what the server does today.
	Summary string
}

// The known gaps. Generators attach these to the cells they affect.
var (
	// GapNumericStrings: a numeric parameter of a $expression tuple accepts any
	// decimal string, so "007", "7.0" and "7e0" all become 7, and a string is
	// accepted for a double. The target accepts only canonical integer strings
	// for int and uint and no strings for double.
	GapNumericStrings = &Gap{Improvement: 1, Summary: "numeric parameters accept non-canonical numeric strings"}

	// GapUnsafeIntegers: a JSON number at or above 2^53 has already been rounded
	// when the server reads it. The target rejects such a number for int and
	// uint parameters; it has to be sent as a string.
	GapUnsafeIntegers = &Gap{Improvement: 1, Summary: "integers at or above 2^53 sent as JSON numbers are accepted after rounding"}

	// GapAnyTypes: a $expression parameter may be declared any, map<any> or
	// list<any>. The target rejects these types when the tuple is written.
	GapAnyTypes = &Gap{Improvement: 2, Summary: "any-typed parameters are accepted in $expression tuples"}

	// GapExtraContextKeys: keys other than expression and parameters in a
	// $expression tuple context are accepted and ignored. The target rejects them.
	GapExtraContextKeys = &Gap{Improvement: 3, Summary: "extra keys in a $expression tuple context are accepted"}

	// GapCostAtWrite: an expression that exceeds the evaluation cost limit for
	// every input is accepted when written. The target rejects it.
	GapCostAtWrite = &Gap{Improvement: 4, Summary: "expressions over the cost limit for any input are accepted at write"}

	// GapAtomicReplace: a tuple's expression cannot be replaced in one request.
	GapAtomicReplace = &Gap{Improvement: 6, Summary: "a tuple cannot be deleted and rewritten in one Write request"}
)

// Provisional reasons. A provisional cell accepts an error besides its
// expected answer until the decision named here is made.
const (
	// ProvisionalTrueWithUnevaluable: a tuple that applies to the user is true
	// and another tuple on the same relation cannot be evaluated. Improvement 7
	// decides whether the answer or an error is correct.
	ProvisionalTrueWithUnevaluable = "improvement 7: a true tuple beside one that cannot be evaluated"

	// ProvisionalUnrelatedUnevaluable: no tuple that applies to the user is
	// true, and a tuple that does not apply to the user cannot be evaluated.
	// Whether that is a deny or an error is undecided.
	ProvisionalUnrelatedUnevaluable = "undecided: a tuple that does not apply to the user cannot be evaluated"
)
