#include <fine.hpp>
#include "tw_hrr.hpp"
#include "tw_loader.hpp"
#include "tw_planner.hpp"
#include "tw_replan.hpp"
#include "tw_temporal.hpp"

#include <stdexcept>
#include <string>
#include <vector>

// Parse a plan JSON array ([[name, arg...], ...]) back to vector<TwCall>.
static std::vector<TwCall> parse_plan(const std::string &p_plan_json) {
	TwValue arr = TwLoader::parse_json_str(p_plan_json);
	std::vector<TwCall> plan;
	if (!arr.is_array()) {
		return plan;
	}
	for (const TwValue &item : arr.as_array()) {
		if (!item.is_array() || item.as_array().empty()) {
			continue;
		}
		TwCall call;
		call.name = item.as_array()[0].as_string();
		for (size_t i = 1; i < item.as_array().size(); ++i) {
			call.args.push_back(item.as_array()[i]);
		}
		plan.push_back(std::move(call));
	}
	return plan;
}

// plan(domain_json) → plan_json
// domain_json is a self-contained JSON-LD document with variables + tasks.
// Raises ErlangError on failure or no-plan.
std::string plan(ErlNifEnv *p_env, std::string p_domain_json) {
	TwLoader::TwLoaded loaded = TwLoader::load_json(p_domain_json);
	if (!loaded.state) {
		throw std::runtime_error("failed_to_load_domain");
	}
	std::optional<std::vector<TwCall>> result = tw_plan(loaded.state, loaded.tasks, loaded.domain);
	if (!result) {
		throw std::runtime_error("no_plan");
	}
	return TwLoader::plan_to_json(*result);
}
FINE_NIF(plan, 0);

// replan(domain_json, plan_json, fail_step) → replan_result_json
// fail_step: 0-based index of the failed action, or -1 to auto-detect.
std::string replan(ErlNifEnv *p_env, std::string p_domain_json,
		std::string p_plan_json, int64_t p_fail_step) {
	TwLoader::TwLoaded loaded = TwLoader::load_json(p_domain_json);
	if (!loaded.state) {
		throw std::runtime_error("failed_to_load_domain");
	}
	std::vector<TwCall> original_plan = parse_plan(p_plan_json);
	TwReplanResult rr = tw_replan(loaded.state, original_plan, loaded.tasks,
			loaded.domain, static_cast<int>(p_fail_step));
	std::string new_plan_json = rr.recovered
			? TwLoader::plan_to_json(*rr.new_plan)
			: "null";
	return tw_replan_to_json(static_cast<int>(p_fail_step), rr,
			TwLoader::plan_to_json(original_plan), new_plan_json);
}
FINE_NIF(replan, 0);

// check_temporal(domain_json, plan_json, origin_iso) → temporal_result_json
// origin_iso: ISO 8601 duration for the plan start offset, e.g. "PT0S".
std::string check_temporal(ErlNifEnv *p_env, std::string p_domain_json,
		std::string p_plan_json, std::string p_origin_iso) {
	TwLoader::TwLoaded loaded = TwLoader::load_json(p_domain_json);
	if (!loaded.state) {
		throw std::runtime_error("failed_to_load_domain");
	}
	std::vector<TwCall> plan_vec = parse_plan(p_plan_json);
	TwTemporalResult tr = tw_check_temporal(plan_vec, loaded.domain, p_origin_iso);
	return tw_temporal_to_json(plan_vec, tr, TwLoader::plan_to_json(plan_vec));
}
FINE_NIF(check_temporal, 0);

// hrr_encode_atom(word, dim) → list of phase angles (floats, radians)
// dim: vector dimension, default 4096. Must be consistent across all calls.
std::vector<double> hrr_encode_atom(ErlNifEnv *p_env, std::string p_word,
		int64_t p_dim) {
	return TwHRR::encode_atom(p_word, static_cast<int>(p_dim));
}
FINE_NIF(hrr_encode_atom, 0);

// hrr_similarity(a, b) → float in [-1, 1]
// Both vectors must have the same dimension.
double hrr_similarity(ErlNifEnv *p_env, std::vector<double> p_a,
		std::vector<double> p_b) {
	return TwHRR::similarity(p_a, p_b);
}
FINE_NIF(hrr_similarity, 0);

// hrr_encode_text(text, dim) → list of phase angles (floats, radians)
// Bag-of-words bundle of token atom vectors.
std::vector<double> hrr_encode_text(ErlNifEnv *p_env, std::string p_text,
		int64_t p_dim) {
	return TwHRR::encode_text(p_text, static_cast<int>(p_dim));
}
FINE_NIF(hrr_encode_text, 0);

// hrr_encode_binding(content, entity, dim) → binary (little-endian float64 bytes)
// content ⊗ encode_atom(entity.lower())
std::string hrr_encode_binding(ErlNifEnv *p_env, std::string p_content,
		std::string p_entity, int64_t p_dim) {
	TwHRR::PhaseVec phases = TwHRR::encode_binding(p_content, p_entity, static_cast<int>(p_dim));
	std::vector<uint8_t> bytes = TwHRR::phases_to_bytes(phases);
	return std::string(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}
FINE_NIF(hrr_encode_binding, 0);

// hrr_encode_fact(content, entities, dim) → binary (little-endian float64 bytes)
// Role-vector bundled encoding.
std::string hrr_encode_fact(ErlNifEnv *p_env, std::string p_content,
		std::vector<std::string> p_entities, int64_t p_dim) {
	TwHRR::PhaseVec phases = TwHRR::encode_fact(p_content, p_entities, static_cast<int>(p_dim));
	std::vector<uint8_t> bytes = TwHRR::phases_to_bytes(phases);
	return std::string(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}
FINE_NIF(hrr_encode_fact, 0);

// hrr_phases_to_bytes(phases) → binary (little-endian float64 bytes)
std::string hrr_phases_to_bytes(ErlNifEnv *p_env, std::vector<double> p_phases) {
	std::vector<uint8_t> bytes = TwHRR::phases_to_bytes(p_phases);
	return std::string(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}
FINE_NIF(hrr_phases_to_bytes, 0);

// hrr_bytes_to_phases(data, len) → list of phase angles
// len: number of phases (not bytes). Pass 0 to infer from binary size.
std::vector<double> hrr_bytes_to_phases(ErlNifEnv *p_env, std::string p_data,
		int64_t p_len) {
	const uint8_t *ptr = reinterpret_cast<const uint8_t *>(p_data.data());
	size_t byte_len = p_len > 0 ? static_cast<size_t>(p_len) * 8 : p_data.size();
	return TwHRR::bytes_to_phases(ptr, byte_len);
}
FINE_NIF(hrr_bytes_to_phases, 0);

FINE_INIT("Elixir.Taskweft.NIF");
