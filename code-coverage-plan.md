# Code Coverage Analysis Guide

## How to Analyze Code Coverage for AuthZEN Implementation

This document explains how to generate and analyze code coverage data to identify uncovered lines in the codebase.

---

## Prerequisites

1. **Install mockgen** (required for generating mocks):
   ```bash
   go install go.uber.org/mock/mockgen@latest
   ```

2. **Ensure mockgen is in PATH**:
   ```bash
   export PATH="$HOME/go/bin:$PATH"
   ```

---

## Method 1: Using Make (Preferred)

The project has a `make test` command that handles coverage generation:

```bash
# Run all tests with coverage
make test

# Run specific tests with coverage
make test FILTER="TestEvaluation|TestEvaluations|TestActionSearch"
```

**Note:** This requires `mockgen` to be available in your PATH. The make command:
- Generates mocks
- Runs tests with race detection
- Generates coverage profile excluding mock files
- Outputs to `coverageunit.out`

---

## Method 2: Direct Go Test (If Make Fails)

If you encounter issues with `make` (e.g., mockgen path problems), run tests directly:

```bash
# Run tests with coverage for specific test pattern
go test -race \
    -run "TestEvaluation|TestEvaluations|TestSubjectSearch|TestResourceSearch|TestActionSearch|TestIsAuthZenEnabled|TestGetConfiguration" \
    -coverpkg=./... \
    -coverprofile=coverageunit.tmp.out \
    -covermode=atomic \
    -count=1 \
    -timeout=5m \
    ./pkg/server
```

**Flags explained:**
- `-race`: Enables race detector
- `-run`: Pattern to match test names
- `-coverpkg=./...`: Collect coverage for all packages
- `-coverprofile`: Output file for coverage data
- `-covermode=atomic`: Thread-safe coverage mode
- `-count=1`: Disable test caching
- `-timeout=5m`: Maximum test duration

---

## Analyzing Coverage Results

### Step 1: View Coverage by Function

```bash
go tool cover -func=coverageunit.tmp.out | grep "pkg/server/authzen.go"
```

**Output example:**
```
github.com/openfga/openfga/pkg/server/authzen.go:20:   IsAuthZenEnabled          100.0%
github.com/openfga/openfga/pkg/server/authzen.go:24:   Evaluation                86.7%
github.com/openfga/openfga/pkg/server/authzen.go:59:   Evaluations               73.9%
github.com/openfga/openfga/pkg/server/authzen.go:112:  evaluateWithShortCircuit  82.8%
github.com/openfga/openfga/pkg/server/authzen.go:188:  SubjectSearch             70.0%
github.com/openfga/openfga/pkg/server/authzen.go:216:  ResourceSearch            70.0%
github.com/openfga/openfga/pkg/server/authzen.go:244:  ActionSearch              39.3%
```

### Step 2: Find Uncovered Lines

```bash
grep "pkg/server/authzen.go" coverageunit.tmp.out | grep " 0$"
```

**Output format:**
```
github.com/openfga/openfga/pkg/server/authzen.go:45.16,47.3 1 0
```

This means:
- `45.16,47.3`: Lines 45-47, columns 16-3
- `1`: Number of statements in this block
- `0`: Execution count (0 = uncovered)

### Step 3: Generate HTML Coverage Report

```bash
go tool cover -html=coverageunit.tmp.out -o coverage.html
open coverage.html
```

This creates an interactive HTML report showing:
- Green: Covered lines
- Red: Uncovered lines
- Gray: Not executable (comments, declarations)

---

## Understanding Coverage Data Format

The coverage file format is:
```
filename:startLine.startCol,endLine.endCol numStatements executionCount
```

**Example:**
```
github.com/openfga/openfga/pkg/server/authzen.go:54.2,56.8 1 0
```
- **Lines 54-56**: Code block range
- **1**: One statement in this block
- **0**: Never executed (uncovered)

---

## Findings for pkg/server/authzen.go

### Current Coverage by Function

| Function | Coverage | Status |
|----------|----------|--------|
| IsAuthZenEnabled | 100.0% | ✅ Fully covered |
| Evaluation | 86.7% | ⚠️ Missing success path |
| Evaluations | 73.9% | ⚠️ Missing success path |
| evaluateWithShortCircuit | 82.8% | ⚠️ Missing permit scenarios |
| SubjectSearch | 70.0% | ⚠️ Missing query execution |
| ResourceSearch | 70.0% | ⚠️ Missing query execution |
| ActionSearch | 39.3% | ❌ Lowest coverage |

**Overall File Coverage: 74.7%**

### Uncovered Lines (from actual coverage data)

#### Evaluation (86.7% coverage)
- **Lines 45-47**: Error handling in `NewEvaluateRequestCommand`
- **Lines 54-56**: Success return building `EvaluationResponse`

**Why uncovered:** Tests only cover error scenarios; success path requires working Check operation.

#### Evaluations (73.9% coverage)
- **Lines 69-71**: Validation error handling
- **Lines 92-94**: `NewBatchEvaluateRequestCommand` error
- **Lines 101-106**: `TransformResponse` success path

**Why uncovered:** Tests only cover validation errors; batch success requires working BatchCheck.

#### evaluateWithShortCircuit (82.8% coverage)
- **Line 169**: Append successful evaluation response
- **Lines 172-174**: `DENY_ON_FIRST_DENY` break on actual deny
- **Lines 176-178**: `PERMIT_ON_FIRST_PERMIT` break on actual permit

**Why uncovered:** Current tests only trigger error scenarios (no model), not actual permit/deny decisions.

#### SubjectSearch (70.0% coverage)
- **Lines 203-212**: Complete query execution success path

**Why uncovered:** Requires working ListUsers operation with actual data.

#### ResourceSearch (70.0% coverage)
- **Lines 231-240**: Complete query execution success path

**Why uncovered:** Requires working StreamedListObjects operation with actual data.

#### ActionSearch (39.3% coverage) ⚠️ LOWEST
- **Lines 271-278**: Check resolver building
- **Lines 279-305**: Check function creation with typesystem
- **Lines 310-312**: Cached typesystem resolver
- **Lines 315-322**: Query execution

**Why uncovered:** Entire success flow requires typesystem resolution, check resolver, and working Check operations.

---

## What Makes Code Uncovered?

All uncovered lines fall into **SUCCESS PATHS** that require:

1. ✅ Valid authorization models in the datastore
2. ✅ Successful Check/BatchCheck/ListUsers/ListObjects operations
3. ✅ Actual data (tuples) for queries to return results

**Current unit tests (`authzen_test.go`) cover:**
- ✅ Feature flag enforcement
- ✅ Validation errors (missing required fields)
- ✅ Error propagation (model not found, invalid input)
- ✅ Short-circuit error handling

**Current unit tests DO NOT cover:**
- ❌ Successful Check responses
- ❌ Successful batch operations
- ❌ Successful query executions

**Integration tests (`/tests/authzen/`) cover:**
- ✅ All success scenarios comprehensively
- ✅ End-to-end flows with real data
- ✅ AuthZEN spec compliance

---

## Options to Increase Coverage

### Option 1: Rely on Integration Tests (Current Approach)
**Status:** ✅ **ACCEPTABLE**

**Rationale:**
- Integration tests comprehensively cover all success paths
- Unit tests focus on error handling and edge cases
- 74.7% unit test coverage is reasonable for server endpoint code
- Success paths are straightforward returns with minimal logic

**Recommendation:** Keep current approach unless Codecov policy requires higher unit test coverage.

### Option 2: Add Mock-Based Unit Tests
**Status:** ⚠️ **OPTIONAL**

**When to consider:**
- Codecov requires >80% unit test coverage for authzen.go
- You want isolated tests for ActionSearch complex logic
- Team prefers comprehensive unit test coverage

**Targets for improvement:**
1. **ActionSearch** (39.3% → 80%+): Mock typesystem resolver and Check function
2. **evaluateWithShortCircuit** (82.8% → 95%+): Mock Check to return success with permit/deny
3. **Search endpoints** (70% → 85%+): Mock ListUsers/ListObjects

**Trade-offs:**
- ➕ Higher unit test coverage percentage
- ➕ Isolated testing of logic without datastore
- ➖ Duplicates integration test coverage
- ➖ Maintenance burden (mocks must match interface changes)
- ➖ Less realistic than integration tests

### Option 3: Hybrid Approach (Recommended if improving coverage)
Add **minimal** mocks for lowest coverage areas:

1. **Focus on ActionSearch (39.3%)**:
   - This single function brings down overall coverage significantly
   - Adding one mock-based success test could bring it to ~80%
   - Would increase overall file coverage from 74.7% to ~83%

2. **Add success scenarios for short-circuit**:
   - Test DENY_ON_FIRST_DENY actually breaks on deny
   - Test PERMIT_ON_FIRST_PERMIT actually breaks on permit

---

## Reproducing This Analysis

To reproduce the analysis performed:

```bash
# 1. Install mockgen
go install go.uber.org/mock/mockgen@latest

# 2. Add to PATH
export PATH="$HOME/go/bin:$PATH"

# 3. Run tests with coverage
go test -race \
    -run "TestEvaluation|TestEvaluations|TestSubjectSearch|TestResourceSearch|TestActionSearch|TestIsAuthZenEnabled|TestGetConfiguration" \
    -coverpkg=./... \
    -coverprofile=coverageunit.tmp.out \
    -covermode=atomic \
    -count=1 \
    -timeout=5m \
    ./pkg/server

# 4. View function coverage
go tool cover -func=coverageunit.tmp.out | grep "pkg/server/authzen.go"

# 5. Find uncovered lines
grep "pkg/server/authzen.go" coverageunit.tmp.out | grep " 0$"

# 6. Generate HTML report
go tool cover -html=coverageunit.tmp.out -o coverage.html
open coverage.html
```

---

## Common Issues and Solutions

### Issue: "mockgen: executable file not found"
**Solution:**
```bash
go install go.uber.org/mock/mockgen@latest
export PATH="$HOME/go/bin:$PATH"
```

### Issue: "missing go.sum entry for module providing package github.com/golang/mock/gomock"
**Solution:**
```bash
go mod tidy
```

### Issue: Mock files have build errors
**Solution:**
```bash
make generate-mocks  # Regenerate with correct mockgen version
```

### Issue: Coverage shows 0% or very low
**Solution:**
- Ensure test filter matches your test names
- Run without filter to check if tests execute at all
- Check that tests are in `*_test.go` files

---

## Conclusion

**Current Status:**
- ✅ Unit tests: 74.7% coverage of authzen.go
- ✅ Integration tests: Comprehensive coverage of success paths
- ✅ Error paths: Well covered in unit tests
- ❌ Success paths: Not covered in unit tests (by design)

**Recommendation:**
Maintain current approach unless:
1. Codecov policy requires >80% unit test coverage, OR
2. Team decides to add mock-based tests for ActionSearch

**If improving coverage, prioritize:**
1. ActionSearch (39.3%) - biggest impact on overall coverage
2. Short-circuit success breaks (82.8%) - important logic to verify

---

## References

- [Go Coverage Documentation](https://pkg.go.dev/cmd/cover)
- [Go Testing Best Practices](https://go.dev/doc/tutorial/add-a-test)
- [Integration Tests Location](/tests/authzen/)
- [Unit Tests Location](/pkg/server/authzen_test.go)
