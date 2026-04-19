// Minimal TwValue → JSON serializer. No Godot dependency.
// Used by tw_rebac, tw_retriever, and tw_bridge.
#pragma once
#include "tw_value.hpp"
#include <sstream>
#include <string>

namespace TwJson {

inline std::string escape_string(const std::string &p_s) {
	std::ostringstream oss;
	oss << '"';
	for (char c : p_s) {
		if (c == '"')       { oss << "\\\""; }
		else if (c == '\\') { oss << "\\\\"; }
		else if (c == '\n') { oss << "\\n"; }
		else if (c == '\r') { oss << "\\r"; }
		else if (c == '\t') { oss << "\\t"; }
		else                { oss << c; }
	}
	oss << '"';
	return oss.str();
}

inline std::string to_json(const TwValue &p_v) {
	std::ostringstream oss;
	switch (p_v.type()) {
		case TwValue::Type::NIL:
			return "null";
		case TwValue::Type::BOOL:
			return p_v.as_bool() ? "true" : "false";
		case TwValue::Type::INT:
			oss << p_v.as_int();
			return oss.str();
		case TwValue::Type::FLOAT:
			oss << p_v.as_float();
			return oss.str();
		case TwValue::Type::STRING:
			return escape_string(p_v.as_string());
		case TwValue::Type::ARRAY: {
			oss << '[';
			bool first = true;
			for (const TwValue &item : p_v.as_array()) {
				if (!first) { oss << ','; }
				oss << to_json(item);
				first = false;
			}
			oss << ']';
			return oss.str();
		}
		case TwValue::Type::DICT: {
			oss << '{';
			bool first = true;
			for (const auto &[key, val] : p_v.as_dict()) {
				if (!first) { oss << ','; }
				oss << escape_string(key) << ':' << to_json(val);
				first = false;
			}
			oss << '}';
			return oss.str();
		}
	}
	return "null";
}

} // namespace TwJson
