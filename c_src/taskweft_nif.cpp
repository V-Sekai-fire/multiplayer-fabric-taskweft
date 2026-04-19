#include <fine.hpp>
#include "tw_loader.hpp"
#include "tw_planner.hpp"
#include "tw_replan.hpp"
#include "tw_temporal.hpp"
#include "tw_hrr.hpp"

#include <stdexcept>
#include <string>
#include <vector>

// Parse a plan JSON array ([[name, arg...], ...]) back to vector<TwCall>.
static std::vector<TwCall> parse_plan(const std::string &plan_json) {
    TwValue arr = TwLoader::parse_json_str(plan_json);
    std::vector<TwCall> plan;
    if (!arr.is_array()) return plan;
    for (const TwValue &item : arr.as_array()) {
        if (!item.is_array() || item.as_array().empty()) continue;
        TwCall call;
        call.name = item.as_array()[0].as_string();
        for (size_t i = 1; i < item.as_array().size(); ++i)
            call.args.push_back(item.as_array()[i]);
        plan.push_back(std::move(call));
    }
    return plan;
}

// plan(domain_json) → plan_json
// domain_json is a self-contained JSON-LD document with variables + tasks.
// Raises ErlangError on failure or no-plan.
std::string plan(ErlNifEnv *env, std::string domain_json) {
    TwLoader::TwLoaded loaded = TwLoader::load_json(domain_json);
    if (!loaded.state) throw std::runtime_error("failed_to_load_domain");
    auto result = tw_plan(loaded.state, loaded.tasks, loaded.domain);
    if (!result) throw std::runtime_error("no_plan");
    return TwLoader::plan_to_json(*result);
}
FINE_NIF(plan, 0);

// replan(domain_json, plan_json, fail_step) → replan_result_json
// fail_step: 0-based index of the failed action, or -1 to auto-detect.
std::string replan(ErlNifEnv *env, std::string domain_json,
                   std::string plan_json, int64_t fail_step) {
    TwLoader::TwLoaded loaded = TwLoader::load_json(domain_json);
    if (!loaded.state) throw std::runtime_error("failed_to_load_domain");
    std::vector<TwCall> original_plan = parse_plan(plan_json);
    TwReplanResult rr = tw_replan(loaded.state, original_plan, loaded.tasks,
                                   loaded.domain, static_cast<int>(fail_step));
    std::string new_plan_json = rr.recovered
        ? TwLoader::plan_to_json(*rr.new_plan) : "null";
    return tw_replan_to_json(static_cast<int>(fail_step), rr,
                              TwLoader::plan_to_json(original_plan), new_plan_json);
}
FINE_NIF(replan, 0);

// check_temporal(domain_json, plan_json, origin_iso) → temporal_result_json
// origin_iso: ISO 8601 duration for the plan start offset, e.g. "PT0S".
std::string check_temporal(ErlNifEnv *env, std::string domain_json,
                            std::string plan_json, std::string origin_iso) {
    TwLoader::TwLoaded loaded = TwLoader::load_json(domain_json);
    if (!loaded.state) throw std::runtime_error("failed_to_load_domain");
    std::vector<TwCall> plan_vec = parse_plan(plan_json);
    TwTemporalResult tr = tw_check_temporal(plan_vec, loaded.domain, origin_iso);
    return tw_temporal_to_json(plan_vec, tr, TwLoader::plan_to_json(plan_vec));
}
FINE_NIF(check_temporal, 0);

// hrr_encode_atom(word, dim) → list of phase angles (floats, radians)
// dim: vector dimension, default 4096. Must be consistent across all calls.
std::vector<double> hrr_encode_atom(ErlNifEnv *env, std::string word,
                                    int64_t dim) {
    return TwHRR::encode_atom(word, static_cast<int>(dim));
}
FINE_NIF(hrr_encode_atom, 0);

// hrr_similarity(a, b) → float in [-1, 1]
// Both vectors must have the same dimension.
double hrr_similarity(ErlNifEnv *env, std::vector<double> a,
                      std::vector<double> b) {
    return TwHRR::similarity(a, b);
}
FINE_NIF(hrr_similarity, 0);

FINE_INIT("Elixir.Taskweft.NIF");
