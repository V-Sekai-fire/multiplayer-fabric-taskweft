// Elixir NIF that calls the Lean-produced GRAFCET static analyser
// through the C bridge in `taskweft-grafcet-static/libgrafcet_static.*`.
//
// The library exposes:
//   char *grafcet_static_analyse(const char *sfc_json);
//   void  grafcet_static_free(char *buf);
//
// This NIF wraps that pair: takes an Elixir iodata SFC, returns the
// analyser's reply as an Elixir binary. Runs on a dirty CPU scheduler
// because the BFS is CPU-bound.
//
// SPDX-License-Identifier: MIT OR Apache-2.0
#include <erl_nif.h>

#include <cstring>

extern "C" {
    char *grafcet_static_analyse(const char *sfc_json);
    void grafcet_static_free(char *buf);
}

namespace {

ERL_NIF_TERM nif_analyse(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    if (argc != 1) return enif_make_badarg(env);

    ErlNifBinary body;
    if (!enif_inspect_iolist_as_binary(env, argv[0], &body)) {
        return enif_make_badarg(env);
    }

    // Zero-terminate the input.
    char* nullterm = static_cast<char*>(enif_alloc(body.size + 1));
    if (!nullterm) return enif_raise_exception(env, enif_make_atom(env, "alloc"));
    std::memcpy(nullterm, body.data, body.size);
    nullterm[body.size] = '\0';

    char* reply = grafcet_static_analyse(nullterm);
    enif_free(nullterm);
    if (!reply) {
        return enif_raise_exception(env, enif_make_atom(env, "analyse_null"));
    }

    ERL_NIF_TERM out;
    const std::size_t n = std::strlen(reply);
    unsigned char* dst = enif_make_new_binary(env, n, &out);
    std::memcpy(dst, reply, n);
    grafcet_static_free(reply);

    ERL_NIF_TERM ok = enif_make_atom(env, "ok");
    return enif_make_tuple2(env, ok, out);
}

ErlNifFunc nif_funcs[] = {
    {"analyse", 1, nif_analyse, ERL_NIF_DIRTY_JOB_CPU_BOUND},
};

}  // namespace

ERL_NIF_INIT(Elixir.Taskweft.Grafcet.Static.Nif, nif_funcs, nullptr, nullptr, nullptr, nullptr)
