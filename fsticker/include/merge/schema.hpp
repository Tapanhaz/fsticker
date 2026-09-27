// Copyright (c) 2026 Tapanhaz
// Part of fsticker :: https://github.com/Tapanhaz/fsticker
// Licensed under the Apache License, Version 2.0
// Created: 2026-09-25 10:41 IST

#pragma once

#include "merge/tick.hpp"

#include <string>
#include <unordered_map>

namespace fsticker::merge {
    using Schema = std::unordered_map<std::string, FieldType>;

    namespace {

        using F = FieldType;

        Schema make_touchline_tick() {
            return {
                {     "e", F::String},
                {    "tk", F::String},
                {    "lp",  F::Float},
                {    "pc",  F::Float},
                {     "v",    F::Int},
                {     "o",  F::Float},
                {     "h",  F::Float},
                {     "l",  F::Float},
                {     "c",  F::Float},
                {    "ap",  F::Float},
                {    "oi",    F::Int},
                {   "poi",    F::Int},
                {   "toi",    F::Int},
                {   "bq1",    F::Int},
                {   "bp1",  F::Float},
                {   "sq1",    F::Int},
                {   "sp1",  F::Float},
                {    "ft",    F::Int},
                {"broker", F::String},
            };
        }

        Schema make_touchline_ack() {
            Schema s = make_touchline_tick();
            s.insert({
                {"ts", F::String},
                {"pp",    F::Int},
                {"ti",  F::Float},
                {"ls",    F::Int}
            });
            return s;
        }

        Schema make_depth_tick() {
            Schema s = {
                {     "e", F::String},
                {    "tk", F::String},
                {    "ts", F::String},
                {    "pp", F::String},
                {    "ls", F::String},
                {    "ti", F::String},
                {    "lp",  F::Float},
                {    "pc",  F::Float},
                {     "v",    F::Int},
                {     "o",  F::Float},
                {     "h",  F::Float},
                {     "l",  F::Float},
                {     "c",  F::Float},
                {    "ap",  F::Float},
                {   "ltt", F::String},
                {   "ltq",    F::Int},
                {   "tbq",    F::Int},
                {   "tsq",    F::Int},
                {    "lc",  F::Float},
                {    "uc",  F::Float},
                {    "oi",    F::Int},
                {   "poi",    F::Int},
                {   "toi",    F::Int},
                {    "ft",    F::Int},
                {"broker", F::String},
            };
            for (int i = 1; i <= 5; ++i) {
                const std::string n = std::to_string(i);
                s["bq" + n]         = F::Int;
                s["bp" + n]         = F::Float;
                s["bo" + n]         = F::Int;
                s["sq" + n]         = F::Int;
                s["sp" + n]         = F::Float;
                s["so" + n]         = F::Int;
            }
            return s;
        }

        Schema make_depth_ack() {
            Schema s = make_depth_tick();
            s.insert({
                {         "ml", F::String},
                {"week52_high",  F::Float},
                { "week52_low",  F::Float}
            });
            return s;
        }

        const Schema &touchline_tick_schema() {
            static const Schema s = make_touchline_tick();
            return s;
        }

        const Schema &touchline_ack_schema() {
            static const Schema s = make_touchline_ack();
            return s;
        }

        const Schema &depth_tick_schema() {
            static const Schema s = make_depth_tick();
            return s;
        }

        const Schema &depth_ack_schema() {
            static const Schema s = make_depth_ack();
            return s;
        }

        const Schema &empty_schema() {
            static const Schema s;
            return s;
        }

    } // namespace

    [[nodiscard]] const Schema &schema_for_tag(const std::string &tag) {
        if (tag == "tf")
            return touchline_tick_schema();
        if (tag == "tk")
            return touchline_ack_schema();
        if (tag == "df")
            return depth_tick_schema();
        if (tag == "dk")
            return depth_ack_schema();
        return empty_schema();
    }

} // namespace fsticker::merge
