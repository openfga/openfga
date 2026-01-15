---
name: analyze-coverage
description: Analyze code coverage for Go packages and suggest improvements with incremental test addition.
---

# Analyze Coverage

Analyze code coverage for Go packages and suggest improvements.

## Usage

```
/analyze-coverage [package-path] [test-pattern]
```

**Examples:**
- `/analyze-coverage ./pkg/server TestEvaluation|TestEvaluations`
- `/analyze-coverage ./pkg/server` (runs all tests)
- `/analyze-coverage` (analyzes main package with existing coverage file)

## What This Skill Does

1. **Runs tests with coverage** for the specified package and test pattern
2. **Analyzes coverage results** to identify uncovered lines
3. **Reports coverage by function** showing percentages
4. **Identifies specific uncovered line ranges**
5. **Suggests tests to add** based on coverage gaps
6. **Optionally adds tests incrementally** and validates each improves coverage

## Instructions

When this skill is invoked, follow these steps:

### Step 1: Setup and Prerequisites

Check that mockgen is available (required for generating mocks):

```bash
which mockgen || echo "mockgen not found - may need to install"
```

If mockgen is missing, guide the user to install it:
```bash
go install go.uber.org/mock/mockgen@latest
export PATH="$HOME/go/bin:$PATH"
```

### Step 2: Parse Arguments

Extract arguments:
- `package-path`: The Go package to test (default: `./pkg/server`)
- `test-pattern`: Optional regex pattern to filter tests (e.g., `TestEvaluation|TestEvaluations`)

### Step 3: Run Tests with Coverage

**If test pattern is provided:**
```bash
go test -race \
    -run "<test-pattern>" \
    -coverpkg=./... \
    -coverprofile=coverageunit.tmp.out \
    -covermode=atomic \
    -count=1 \
    -timeout=5m \
    <package-path>
```

**If no test pattern (run all tests):**
```bash
go test -race \
    -coverpkg=./... \
    -coverprofile=coverageunit.tmp.out \
    -covermode=atomic \
    -count=1 \
    -timeout=5m \
    <package-path>
```

### Step 4: Analyze Coverage Results

#### 4a. Show Overall Coverage Summary

```bash
go tool cover -func=coverageunit.tmp.out | tail -1
```

#### 4b. Show Coverage by File

If user specified a particular file of interest, filter to that file:
```bash
go tool cover -func=coverageunit.tmp.out | grep "<filename>"
```

Otherwise show all files with coverage data.

#### 4c. Identify Uncovered Lines

For the target file(s):
```bash
grep "<filename>" coverageunit.tmp.out | grep " 0$"
```

Parse the output format: `filename:startLine.startCol,endLine.endCol numStatements executionCount`

Example: `pkg/server/authzen.go:45.16,47.3 1 0` means lines 45-47 are uncovered.

### Step 5: Generate Analysis Report

Create a markdown report with:

1. **Coverage Summary Table**
   - List each function with its coverage percentage
   - Mark status: ✅ (>90%), ⚠️ (70-90%), ❌ (<70%)

2. **Uncovered Lines Detail**
   - Group by function
   - Show specific line ranges that are uncovered
   - Use the Read tool to examine those lines and explain WHY they're uncovered

3. **Categorize Missing Coverage**
   - Success paths (need successful operations)
   - Error paths (need specific error conditions)
   - Edge cases (need specific input combinations)
   - Short-circuit logic (need specific decision branches)

### Step 6: Suggest Improvements

Based on the analysis, suggest specific tests to add:

1. **Prioritize by impact**: Focus on functions with lowest coverage
2. **Be specific**: "Add test for ActionSearch success path with valid model and tuples"
3. **Explain what's needed**: "Requires mocking typesystem resolver to return valid types"
4. **Estimate impact**: "Should increase coverage from 39.3% → ~80%"

### Step 7: Interactive Test Addition (Optional)

Ask the user: "Would you like me to add these tests one by one and measure impact?"

If yes:
1. Add the first suggested test
2. Run coverage again to measure impact
3. Report the coverage change (e.g., "39.3% → 92.9% (+53.6%)")
4. Ask: "Coverage increased. Keep this test and continue?"
5. If yes and coverage increased: Keep test, move to next
6. If coverage didn't increase: Remove test, explain it's redundant

Continue until all suggestions are processed or user stops.

## Coverage Data Format Reference

Coverage file format:
```
filename:startLine.startCol,endLine.endCol numStatements executionCount
```

- `executionCount = 0`: Line is uncovered
- `executionCount > 0`: Line is covered

Example:
```
github.com/openfga/openfga/pkg/server/authzen.go:54.2,56.8 1 0
```
- Lines 54-56 (one statement) never executed

## Common Issues and Solutions

### Issue: mockgen not found
**Solution:**
```bash
go install go.uber.org/mock/mockgen@latest
export PATH="$HOME/go/bin:$PATH"
```

### Issue: Missing go.sum entry
**Solution:**
```bash
go mod tidy
```

### Issue: Coverage shows 0% or very low
**Solution:**
- Check test filter matches actual test names
- Run without filter to verify tests execute
- Ensure tests are in `*_test.go` files

### Issue: Mock files have build errors
**Solution:**
```bash
make generate-mocks
```

## Best Practices

1. **Unit vs Integration Tests**
   - Unit tests (in `pkg/*_test.go`): Focus on error paths, edge cases, and isolated logic
   - Integration tests (in `tests/*/`): Cover success paths with real operations
   - Both contribute to overall coverage

2. **When to Add Unit Tests**
   - Success paths that are complex business logic (not just return statements)
   - Error paths that aren't covered by integration tests
   - Edge cases and boundary conditions
   - Short-circuit logic that requires specific branches

3. **When NOT to Add Unit Tests**
   - Success paths already covered by integration tests (avoid duplication)
   - Simple return statements with no logic
   - Code that only calls other functions without decisions

4. **Incremental Addition**
   - Always measure impact before keeping a test
   - Only keep tests that increase coverage
   - Avoid redundant tests that duplicate integration test coverage

## Output Format

Provide output in this structure:

```markdown
## Coverage Analysis Report

### Package: <package-path>
### Test Pattern: <pattern or "all tests">
### Generated: <timestamp>

---

## Overall Coverage
- **Total Coverage**: X.X%
- **Tests Run**: N passed

---

## Coverage by Function

| Function | Coverage | Status |
|----------|----------|--------|
| FunctionName | XX.X% | ✅/⚠️/❌ |

---

## Uncovered Lines

### FunctionName (XX.X% coverage)

**Lines X-Y**: Description of what this code does
**Lines A-B**: Description of what this code does

**Why uncovered:** Explanation

---

## Recommendations

### Priority 1: FunctionName (XX.X% coverage)
**Add test:** Specific test description
**What's needed:** Mock/setup requirements
**Expected impact:** XX.X% → ~YY.Y% (+ZZ.Z%)

### Priority 2: ...

---

## Next Steps

Would you like me to:
1. Add the suggested tests incrementally with impact measurement?
2. Generate an HTML coverage report (`go tool cover -html`)?
3. Focus on a specific function for detailed analysis?
```

## Notes

- This skill uses the workflow documented in `/code-coverage-plan.md`
- Coverage analysis helps identify gaps but context matters - not all uncovered lines need tests
- Integration tests often cover success paths better than mocked unit tests
- The goal is meaningful coverage, not 100% coverage
- Typically 70-85% unit test coverage for server endpoint code is acceptable when combined with comprehensive integration tests
