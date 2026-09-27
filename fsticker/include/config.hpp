// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST

#pragma once

#include "types.hpp"

#include <fstream>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace fsticker {
    class ConfigError : public std::runtime_error {
    public:
        explicit ConfigError(const std::string &msg) : std::runtime_error(msg) {
        }
    };

    namespace {

        [[nodiscard]] std::string slurp_file(const std::string &path) {
            std::ifstream f(path, std::ios::in | std::ios::binary);
            if (!f) {
                throw ConfigError("fsticker::Config: cannot open file: " + path);
            }
            std::ostringstream ss;
            ss << f.rdbuf();
            return ss.str();
        }

        [[nodiscard]] std::string require_string(const rapidjson::Value &obj,
                                                 const char             *key,
                                                 const std::string      &profile_name) {
            auto it = obj.FindMember(key);
            if (it == obj.MemberEnd() || !it->value.IsString()) {
                throw ConfigError("fsticker::Config: profile '" + profile_name +
                                  "' is missing required string field '" + key + "'");
            }
            return std::string(it->value.GetString(), it->value.GetStringLength());
        }

        [[nodiscard]] bool optional_bool(const rapidjson::Value &obj, const char *key, bool fallback) {
            auto it = obj.FindMember(key);
            if (it == obj.MemberEnd() || !it->value.IsBool())
                return fallback;
            return it->value.GetBool();
        }

        [[nodiscard]] std::string optional_string(const rapidjson::Value &obj,
                                                  const char             *key,
                                                  const std::string      &fallback) {
            auto it = obj.FindMember(key);
            if (it == obj.MemberEnd() || !it->value.IsString())
                return fallback;
            return std::string(it->value.GetString(), it->value.GetStringLength());
        }

    } // namespace

    class Config {
    public:
        static std::unordered_map<std::string, Credentials> load_all(const std::string &path) {
            const std::string text = slurp_file(path);

            rapidjson::Document    doc;
            rapidjson::ParseResult ok = doc.Parse(text.c_str(), text.size());
            if (!ok) {
                throw ConfigError("fsticker::Config: JSON parse error in " + path + " at offset " +
                                  std::to_string(ok.Offset()) + ": " +
                                  rapidjson::GetParseError_En(ok.Code()));
            }
            if (!doc.IsObject()) {
                throw ConfigError("fsticker::Config: top level of " + path + " must be a JSON object");
            }

            auto profiles_it = doc.FindMember("profiles");
            if (profiles_it == doc.MemberEnd() || !profiles_it->value.IsObject()) {
                throw ConfigError("fsticker::Config: " + path +
                                  " must contain a top-level 'profiles' object");
            }

            std::unordered_map<std::string, Credentials> result;
            for (auto it = profiles_it->value.MemberBegin(); it != profiles_it->value.MemberEnd();
                 ++it) {
                const std::string name(it->name.GetString(), it->name.GetStringLength());
                if (!it->value.IsObject()) {
                    throw ConfigError("fsticker::Config: profile '" + name +
                                      "' must be a JSON object");
                }
                const rapidjson::Value &obj = it->value;

                Credentials c;
                c.ws_endpoint = require_string(obj, "ws_endpoint", name);
                c.user_id     = require_string(obj, "user_id", name);
                c.token       = require_string(obj, "token", name);
                c.access_type = access_type_from_string(optional_string(obj, "access_type", "API"));
                c.verify_ssl  = optional_bool(obj, "verify_ssl", false);
                c.enable_ip_pinning = optional_bool(obj, "enable_ip_pinning", false);

                result.emplace(name, std::move(c));
            }

            if (result.empty()) {
                throw ConfigError("fsticker::Config: " + path + " contains no profiles");
            }
            return result;
        }

        static Credentials load_profile(const std::string &path, const std::string &profile_name) {
            auto all = load_all(path);
            auto it  = all.find(profile_name);
            if (it == all.end()) {
                throw ConfigError("fsticker::Config: profile '" + profile_name + "' not found in " +
                                  path);
            }
            return it->second;
        }
    };

} // namespace fsticker
