#include <fine.hpp>
#include "tw_bridge.hpp"
#include "tw_hrr.hpp"
#include "tw_json.hpp"
#include "tw_loader.hpp"
#include "tw_planner.hpp"
#include "tw_rebac.hpp"
#include "tw_replan.hpp"
#include "tw_retriever.hpp"
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

// hrr_bind(a_bytes, b_bytes) → binary (little-endian float64 bytes)
// Circular-addition bind of two phase vectors encoded as bytes.
std::string hrr_bind(ErlNifEnv *p_env, std::string p_a_bytes, std::string p_b_bytes) {
	const uint8_t *ap = reinterpret_cast<const uint8_t *>(p_a_bytes.data());
	const uint8_t *bp = reinterpret_cast<const uint8_t *>(p_b_bytes.data());
	TwHRR::PhaseVec a = TwHRR::bytes_to_phases(ap, p_a_bytes.size());
	TwHRR::PhaseVec b = TwHRR::bytes_to_phases(bp, p_b_bytes.size());
	TwHRR::PhaseVec result = TwHRR::bind(a, b);
	std::vector<uint8_t> out = TwHRR::phases_to_bytes(result);
	return std::string(reinterpret_cast<const char *>(out.data()), out.size());
}
FINE_NIF(hrr_bind, 0);

// hrr_unbind(bound_bytes, key_bytes) → binary (little-endian float64 bytes)
// Exact inverse: circular-subtraction unbind.
std::string hrr_unbind(ErlNifEnv *p_env, std::string p_bound_bytes, std::string p_key_bytes) {
	const uint8_t *bp = reinterpret_cast<const uint8_t *>(p_bound_bytes.data());
	const uint8_t *kp = reinterpret_cast<const uint8_t *>(p_key_bytes.data());
	TwHRR::PhaseVec bound = TwHRR::bytes_to_phases(bp, p_bound_bytes.size());
	TwHRR::PhaseVec key   = TwHRR::bytes_to_phases(kp, p_key_bytes.size());
	TwHRR::PhaseVec result = TwHRR::unbind(bound, key);
	std::vector<uint8_t> out = TwHRR::phases_to_bytes(result);
	return std::string(reinterpret_cast<const char *>(out.data()), out.size());
}
FINE_NIF(hrr_unbind, 0);

// hrr_bundle(vectors_bytes_list) → binary (little-endian float64 bytes)
// Phase-average bundle of a list of byte-encoded phase vectors.
std::string hrr_bundle(ErlNifEnv *p_env, std::vector<std::string> p_vecs) {
	std::vector<TwHRR::PhaseVec> vecs;
	vecs.reserve(p_vecs.size());
	for (const std::string &v : p_vecs) {
		const uint8_t *ptr = reinterpret_cast<const uint8_t *>(v.data());
		vecs.push_back(TwHRR::bytes_to_phases(ptr, v.size()));
	}
	if (vecs.empty()) {
		return std::string();
	}
	TwHRR::PhaseVec result = TwHRR::bundle(vecs);
	std::vector<uint8_t> out = TwHRR::phases_to_bytes(result);
	return std::string(reinterpret_cast<const char *>(out.data()), out.size());
}
FINE_NIF(hrr_bundle, 0);

// rebac_add_edge(graph_json, subj, obj, rel) → graph_json
// Add a directed relation edge and return the updated graph JSON.
std::string rebac_add_edge(ErlNifEnv *p_env, std::string p_graph_json,
		std::string p_subj, std::string p_obj, std::string p_rel) {
	TwReBAC::TwReBACGraph g = TwReBAC::graph_from_json(p_graph_json);
	g.add_edge(p_subj, p_obj, TwReBAC::parse_rel(p_rel));
	return TwReBAC::graph_to_json(g);
}
FINE_NIF(rebac_add_edge, 0);

// rebac_check(graph_json, subj, expr_json, obj, fuel) → bool
// Evaluate a RelationExpr against the graph.
bool rebac_check(ErlNifEnv *p_env, std::string p_graph_json,
		std::string p_subj, std::string p_expr_json,
		std::string p_obj, int64_t p_fuel) {
	TwReBAC::TwReBACGraph g = TwReBAC::graph_from_json(p_graph_json);
	TwValue expr = TwLoader::parse_json_str(p_expr_json);
	return TwReBAC::check_expr(g, p_subj, expr, p_obj, static_cast<int>(p_fuel));
}
FINE_NIF(rebac_check, 0);

// rebac_expand(graph_json, rel, obj, fuel) → list of subject strings
// All subjects that hold rel to obj (direct + IS_MEMBER_OF transitive).
std::vector<std::string> rebac_expand(ErlNifEnv *p_env, std::string p_graph_json,
		std::string p_rel, std::string p_obj, int64_t p_fuel) {
	TwReBAC::TwReBACGraph g = TwReBAC::graph_from_json(p_graph_json);
	return TwReBAC::tw_expand(g, p_rel, p_obj, static_cast<int>(p_fuel));
}
FINE_NIF(rebac_expand, 0);

// rebac_parse_relation_edges(facts_json, trust_threshold) → graph_json
// Extract relation edges from memory fact sentences.
std::string rebac_parse_relation_edges(ErlNifEnv *p_env, std::string p_facts_json,
		double p_trust_threshold) {
	return TwBridge::parse_relation_edges(p_facts_json, p_trust_threshold);
}
FINE_NIF(rebac_parse_relation_edges, 0);

// retriever_score(candidates_json, query_text, query_hrr_bytes,
//                 fts_w, jaccard_w, hrr_w, half_life_days, dim) → scored_json
std::string retriever_score(ErlNifEnv *p_env, std::string p_candidates_json,
		std::string p_query_text, std::string p_query_hrr_bytes,
		double p_fts_w, double p_jaccard_w, double p_hrr_w,
		double p_half_life_days, int64_t p_dim) {
	return TwRetriever::score_candidates(p_candidates_json, p_query_text,
			p_query_hrr_bytes, p_fts_w, p_jaccard_w, p_hrr_w,
			p_half_life_days, p_dim);
}
FINE_NIF(retriever_score, 0);

// retriever_probe(candidates_json, entity_hrr_bytes, dim) → scored_json
// Unbind-based algebraic probe: score by recovered similarity to content.
std::string retriever_probe(ErlNifEnv *p_env, std::string p_candidates_json,
		std::string p_entity_hrr_bytes, int64_t p_dim) {
	return TwRetriever::probe_score(p_candidates_json, p_entity_hrr_bytes, p_dim);
}
FINE_NIF(retriever_probe, 0);

// retriever_reason(candidates_json, entity_hrr_bytes_list, dim) → scored_json
// AND-semantics multi-entity reasoning score (min-sim across entities).
std::string retriever_reason(ErlNifEnv *p_env, std::string p_candidates_json,
		std::vector<std::string> p_entity_hrr_bytes_list, int64_t p_dim) {
	return TwRetriever::reason_score(p_candidates_json, p_entity_hrr_bytes_list, p_dim);
}
FINE_NIF(retriever_reason, 0);

// bridge_binding_content(var, arg, val) → string "var arg val"
std::string bridge_binding_content(ErlNifEnv *p_env, std::string p_var,
		std::string p_arg, std::string p_val) {
	return TwBridge::binding_content(p_var, p_arg, p_val);
}
FINE_NIF(bridge_binding_content, 0);

// bridge_extract_entities(state_json) → list of entity strings
// Inner dict keys from a PDDL-style state, excluding private/rigid vars.
std::vector<std::string> bridge_extract_entities(ErlNifEnv *p_env,
		std::string p_state_json) {
	return TwBridge::extract_state_entities(p_state_json);
}
FINE_NIF(bridge_extract_entities, 0);

// bridge_plan_contents(plan_json, domain, entities_json) → json array
// [{content, category, tags}] for storing a plan result in memory.
std::string bridge_plan_contents(ErlNifEnv *p_env, std::string p_plan_json,
		std::string p_domain, std::string p_entities_json) {
	return TwBridge::plan_result_contents(p_plan_json, p_domain, p_entities_json);
}
FINE_NIF(bridge_plan_contents, 0);

// bridge_state_bindings(state_json, domain, category) → json array
// [{content, category, tags}] for all (var, arg, val) triples in state.
std::string bridge_state_bindings(ErlNifEnv *p_env, std::string p_state_json,
		std::string p_domain, std::string p_category) {
	return TwBridge::state_bindings_contents(p_state_json, p_domain, p_category);
}
FINE_NIF(bridge_state_bindings, 0);

FINE_INIT("Elixir.Taskweft.NIF");
