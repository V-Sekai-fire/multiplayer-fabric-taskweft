// ReBAC computed relation expressions and graph evaluation.
// Pure C++20 port of plan_memory/rebac.py. No Godot dependency.
// JSON wire format mirrors Python: {"type":"base","rel":"OWNS"}, etc.
#pragma once
#include "tw_json.hpp"
#include "tw_loader.hpp"
#include "tw_value.hpp"

#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace TwReBAC {

// ---- RelationType enum ---------------------------------------------------

enum class RelationType {
	HAS_CAPABILITY,
	CONTROLS,
	OWNS,
	IS_MEMBER_OF,
	DELEGATED_TO,
	SUPERVISOR_OF,
	PARTNER_OF,
	UNKNOWN,
};

inline RelationType parse_rel(const std::string &p_s) {
	if (p_s == "HAS_CAPABILITY") { return RelationType::HAS_CAPABILITY; }
	if (p_s == "CONTROLS")       { return RelationType::CONTROLS; }
	if (p_s == "OWNS")           { return RelationType::OWNS; }
	if (p_s == "IS_MEMBER_OF")   { return RelationType::IS_MEMBER_OF; }
	if (p_s == "DELEGATED_TO")   { return RelationType::DELEGATED_TO; }
	if (p_s == "SUPERVISOR_OF")  { return RelationType::SUPERVISOR_OF; }
	if (p_s == "PARTNER_OF")     { return RelationType::PARTNER_OF; }
	return RelationType::UNKNOWN;
}

inline std::string rel_str(RelationType p_r) {
	switch (p_r) {
		case RelationType::HAS_CAPABILITY: return "HAS_CAPABILITY";
		case RelationType::CONTROLS:       return "CONTROLS";
		case RelationType::OWNS:           return "OWNS";
		case RelationType::IS_MEMBER_OF:   return "IS_MEMBER_OF";
		case RelationType::DELEGATED_TO:   return "DELEGATED_TO";
		case RelationType::SUPERVISOR_OF:  return "SUPERVISOR_OF";
		case RelationType::PARTNER_OF:     return "PARTNER_OF";
		default:                           return "UNKNOWN";
	}
}

// ---- Edge and Graph -------------------------------------------------------

struct TwEdge {
	std::string subject;
	std::string object;
	RelationType rel = RelationType::UNKNOWN;
};

struct TwReBACGraph {
	std::vector<TwEdge> edges;
	// subject → edge indices
	std::unordered_map<std::string, std::vector<size_t>> subj_idx;
	// object → edge indices
	std::unordered_map<std::string, std::vector<size_t>> obj_idx;
	// named computed relation definitions (stored as TwValue)
	std::unordered_map<std::string, TwValue> definitions;

	void add_edge(const std::string &p_subj, const std::string &p_obj, RelationType p_rel) {
		size_t idx = edges.size();
		edges.push_back({p_subj, p_obj, p_rel});
		subj_idx[p_subj].push_back(idx);
		obj_idx[p_obj].push_back(idx);
	}

	void define(const std::string &p_name, TwValue p_expr) {
		definitions[p_name] = std::move(p_expr);
	}
};

// ---- Forward declaration --------------------------------------------------

inline bool check_expr(const TwReBACGraph &p_g, const std::string &p_subj,
		const TwValue &p_expr, const std::string &p_obj, int p_fuel);

// ---- Base check: direct edges + IS_MEMBER_OF transitive + CONTROLS delegation ---

inline bool check_base(const TwReBACGraph &p_g, const std::string &p_subj,
		RelationType p_rel, const std::string &p_obj, int p_fuel) {
	if (p_fuel <= 0) {
		return false;
	}

	// Direct edge
	auto sit = p_g.subj_idx.find(p_subj);
	if (sit != p_g.subj_idx.end()) {
		for (size_t idx : sit->second) {
			const TwEdge &e = p_g.edges[idx];
			if (e.rel == p_rel && e.object == p_obj) {
				return true;
			}
		}
		// Transitive IS_MEMBER_OF chain
		for (size_t idx : sit->second) {
			const TwEdge &e = p_g.edges[idx];
			if (e.rel == RelationType::IS_MEMBER_OF) {
				TwValue::Dict m;
				m["type"] = TwValue(std::string("base"));
				m["rel"]  = TwValue(rel_str(p_rel));
				if (check_expr(p_g, e.object, TwValue(std::move(m)), p_obj, p_fuel - 1)) {
					return true;
				}
			}
		}
	}

	// Delegation inversion for CONTROLS
	if (p_rel == RelationType::CONTROLS) {
		auto oit = p_g.obj_idx.find(p_subj);
		if (oit != p_g.obj_idx.end()) {
			for (size_t idx : oit->second) {
				const TwEdge &e = p_g.edges[idx];
				if (e.rel == RelationType::DELEGATED_TO && e.subject == p_obj) {
					return true;
				}
			}
		}
	}

	return false;
}

// ---- check_expr -----------------------------------------------------------

inline bool check_expr(const TwReBACGraph &p_g, const std::string &p_subj,
		const TwValue &p_expr, const std::string &p_obj, int p_fuel) {
	if (p_fuel <= 0 || !p_expr.is_dict()) {
		return false;
	}
	const TwValue::Dict &m = p_expr.as_dict();
	auto tit = m.find("type");
	if (tit == m.end()) {
		return false;
	}
	const std::string &type = tit->second.as_string();

	if (type == "base") {
		auto rit = m.find("rel");
		if (rit == m.end()) {
			return false;
		}
		return check_base(p_g, p_subj, parse_rel(rit->second.as_string()), p_obj, p_fuel);
	}
	if (type == "union") {
		auto ait = m.find("a");
		auto bit = m.find("b");
		if (ait == m.end() || bit == m.end()) {
			return false;
		}
		return check_expr(p_g, p_subj, ait->second, p_obj, p_fuel - 1) ||
				check_expr(p_g, p_subj, bit->second, p_obj, p_fuel - 1);
	}
	if (type == "intersection") {
		auto ait = m.find("a");
		auto bit = m.find("b");
		if (ait == m.end() || bit == m.end()) {
			return false;
		}
		return check_expr(p_g, p_subj, ait->second, p_obj, p_fuel - 1) &&
				check_expr(p_g, p_subj, bit->second, p_obj, p_fuel - 1);
	}
	if (type == "difference") {
		auto ait = m.find("a");
		auto bit = m.find("b");
		if (ait == m.end() || bit == m.end()) {
			return false;
		}
		return check_expr(p_g, p_subj, ait->second, p_obj, p_fuel - 1) &&
				!check_expr(p_g, p_subj, bit->second, p_obj, p_fuel - 1);
	}
	if (type == "tuple_to_userset") {
		auto pit = m.find("pivot_rel");
		auto iit = m.find("inner");
		if (pit == m.end() || iit == m.end()) {
			return false;
		}
		RelationType pivot = parse_rel(pit->second.as_string());
		auto sit = p_g.subj_idx.find(p_subj);
		if (sit == p_g.subj_idx.end()) {
			return false;
		}
		for (size_t idx : sit->second) {
			const TwEdge &e = p_g.edges[idx];
			if (e.rel == pivot) {
				if (check_expr(p_g, e.object, iit->second, p_obj, p_fuel - 1)) {
					return true;
				}
			}
		}
		return false;
	}
	return false;
}

// ---- expand ---------------------------------------------------------------

inline std::vector<std::string> tw_expand(const TwReBACGraph &p_g,
		const std::string &p_rel_str, const std::string &p_obj, int p_fuel = 3) {
	RelationType rel = parse_rel(p_rel_str);
	std::unordered_set<std::string> result;

	// Direct holders
	auto oit = p_g.obj_idx.find(p_obj);
	if (oit != p_g.obj_idx.end()) {
		for (size_t idx : oit->second) {
			const TwEdge &e = p_g.edges[idx];
			if (e.rel == rel) {
				result.insert(e.subject);
			}
		}
	}

	// IS_MEMBER_OF inheritance
	if (p_fuel > 0) {
		for (const TwEdge &e : p_g.edges) {
			if (e.rel == RelationType::IS_MEMBER_OF) {
				TwValue::Dict m;
				m["type"] = TwValue(std::string("base"));
				m["rel"]  = TwValue(rel_str(rel));
				if (check_expr(p_g, e.object, TwValue(std::move(m)), p_obj, p_fuel)) {
					result.insert(e.subject);
				}
			}
		}
	}

	return std::vector<std::string>(result.begin(), result.end());
}

// ---- JSON serialization / deserialization --------------------------------

inline TwReBACGraph graph_from_json(const std::string &p_json) {
	TwValue root = TwLoader::parse_json_str(p_json);
	TwReBACGraph g;
	if (!root.is_dict()) {
		return g;
	}
	const TwValue::Dict &m = root.as_dict();

	auto eit = m.find("edges");
	if (eit != m.end() && eit->second.is_array()) {
		for (const TwValue &ev : eit->second.as_array()) {
			if (!ev.is_dict()) {
				continue;
			}
			const TwValue::Dict &em = ev.as_dict();
			auto sit = em.find("subject");
			auto oit = em.find("object");
			auto rit = em.find("rel");
			if (sit == em.end() || oit == em.end() || rit == em.end()) {
				continue;
			}
			g.add_edge(sit->second.as_string(), oit->second.as_string(),
					parse_rel(rit->second.as_string()));
		}
	}

	auto dit = m.find("definitions");
	if (dit != m.end() && dit->second.is_dict()) {
		for (const auto &[name, expr] : dit->second.as_dict()) {
			g.definitions[name] = expr;
		}
	}

	return g;
}

inline std::string graph_to_json(const TwReBACGraph &p_g) {
	std::ostringstream oss;
	oss << "{\"edges\":[";
	for (size_t i = 0; i < p_g.edges.size(); ++i) {
		if (i) {
			oss << ',';
		}
		const TwEdge &e = p_g.edges[i];
		oss << "{\"subject\":" << TwJson::escape_string(e.subject)
			<< ",\"object\":" << TwJson::escape_string(e.object)
			<< ",\"rel\":" << TwJson::escape_string(rel_str(e.rel))
			<< '}';
	}
	oss << "],\"definitions\":{";
	bool first = true;
	for (const auto &[name, expr] : p_g.definitions) {
		if (!first) {
			oss << ',';
		}
		oss << TwJson::escape_string(name) << ':' << TwJson::to_json(expr);
		first = false;
	}
	oss << "}}";
	return oss.str();
}

} // namespace TwReBAC
