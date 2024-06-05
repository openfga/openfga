// Package dispatch contains code related to dispatching of FGA queries.
//
// FGA queries can be thought of as a tree of evaluations. That is, starting
// with some parent FGA subproblem, we incrementally break the subproblem down
// into child subproblems and visit each of these subproblem branches until we
// find a resolution outcome or until we exhaust the whole tree and find nothing.
//
// Dispatches occur when we must visit subproblem branches in order to resolve some
// parent subproblem. The purpose of this package is to contain signatures related
// to dispatch composition or implementations thereof.
package dispatch
