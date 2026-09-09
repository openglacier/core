#include "chat.h"
#include "common.h"

#include <nlohmann/json.hpp>

#include <cstdlib>
#include <cstring>
#include <exception>
#include <string>
#include <vector>

using json = nlohmann::ordered_json;

namespace {

char * dup_string(const std::string & value) {
    auto * result = static_cast<char *>(std::malloc(value.size() + 1));
    if (!result) {
        return nullptr;
    }
    std::memcpy(result, value.data(), value.size());
    result[value.size()] = '\0';
    return result;
}

int fail(char ** out_error, const std::string & message) {
    if (out_error) {
        *out_error = dup_string(message);
    }
    return -1;
}

std::string regex_escape_local(const std::string & value) {
    static const std::string metacharacters = R"(\.^$|()[]{}*+?)";
    std::string result;
    result.reserve(value.size() * 2);
    for (char ch : value) {
        if (metacharacters.find(ch) != std::string::npos) {
            result.push_back('\\');
        }
        result.push_back(ch);
    }
    return result;
}

std::string anchor_full_pattern(const std::string & pattern) {
    if (pattern.empty()) {
        return "^$";
    }
    return (pattern.front() == '^' ? "" : "^") + pattern + (pattern.back() == '$' ? "" : "$");
}

std::string strip_think_blocks(std::string value) {
    static const std::string open = "<think>";
    static const std::string close = "</think>";

    std::size_t search_from = 0;
    while (true) {
        const auto begin = value.find(open, search_from);
        if (begin == std::string::npos) {
            break;
        }
        const auto end = value.find(close, begin + open.size());
        if (end == std::string::npos) {
            break;
        }
        value.erase(begin, end + close.size() - begin);
        search_from = begin;
    }

    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) {
        return {};
    }
    if (first > 0) {
        value.erase(0, first);
    }
    return value;
}

} // namespace

extern "C" int og_llama_common_chat_apply(
    const char * chat_template,
    const char * bos_token,
    const char * eos_token,
    const char * messages_json,
    const char * tools_json,
    int tool_choice,
    int thinking_mode,
    char ** out_json,
    char ** out_error) {
    if (!chat_template || !messages_json || !tools_json || !out_json) {
        return fail(out_error, "invalid common/chat apply arguments");
    }
    *out_json = nullptr;
    if (out_error) {
        *out_error = nullptr;
    }

    try {
        const auto messages_value = json::parse(messages_json);
        const auto tools_value = json::parse(tools_json);
        auto templates = common_chat_templates_init(
            nullptr,
            chat_template,
            bos_token ? bos_token : "",
            eos_token ? eos_token : "");
        if (!templates) {
            return fail(out_error, "llama.cpp rejected the chat template");
        }

        common_chat_templates_inputs inputs;
        inputs.messages = common_chat_msgs_parse_oaicompat(messages_value);
        inputs.tools = common_chat_tools_parse_oaicompat(tools_value);
        switch (tool_choice) {
            case 0:
                inputs.tool_choice = COMMON_CHAT_TOOL_CHOICE_AUTO;
                break;
            case 1:
                inputs.tool_choice = COMMON_CHAT_TOOL_CHOICE_NONE;
                break;
            case 2:
                inputs.tool_choice = COMMON_CHAT_TOOL_CHOICE_REQUIRED;
                break;
            default:
                return fail(out_error, "invalid common/chat tool choice");
        }
        inputs.parallel_tool_calls = false;
        inputs.add_generation_prompt = true;
        inputs.use_jinja = true;
        int parser_reasoning_format = 0;
        switch (thinking_mode) {
            case 0: // auto: keep llama.cpp common/chat defaults
                break;
            case 1: // on: enable reasoning but keep it separate from user-visible content
                inputs.reasoning_format = COMMON_REASONING_FORMAT_DEEPSEEK;
                inputs.enable_thinking = true;
                parser_reasoning_format = 1;
                break;
            case 2: // off
                inputs.reasoning_format = COMMON_REASONING_FORMAT_NONE;
                inputs.enable_thinking = false;
                // Preserve the normal tool-call parser. A distinct bridge-local
                // mode tells parse() to strip any residual <think> envelope only
                // after common_chat_parse() has extracted native tool calls.
                parser_reasoning_format = 2;
                break;
            default:
                return fail(out_error, "invalid common/chat thinking mode");
        }
        inputs.add_bos = false;
        inputs.add_eos = false;

        const auto params = common_chat_templates_apply(templates.get(), inputs);

        json trigger_patterns = json::array();
        json trigger_tokens = json::array();
        for (const auto & trigger : params.grammar_triggers) {
            switch (trigger.type) {
                case COMMON_GRAMMAR_TRIGGER_TYPE_WORD:
                    trigger_patterns.push_back(regex_escape_local(trigger.value));
                    break;
                case COMMON_GRAMMAR_TRIGGER_TYPE_PATTERN:
                    trigger_patterns.push_back(trigger.value);
                    break;
                case COMMON_GRAMMAR_TRIGGER_TYPE_PATTERN_FULL:
                    trigger_patterns.push_back(anchor_full_pattern(trigger.value));
                    break;
                case COMMON_GRAMMAR_TRIGGER_TYPE_TOKEN:
                    trigger_tokens.push_back(trigger.token);
                    break;
                default:
                    return fail(out_error, "llama.cpp returned an unknown grammar trigger type");
            }
        }

        json result = {
            {"prompt", params.prompt},
            {"grammar", params.grammar},
            {"grammarLazy", params.grammar_lazy},
            // common_chat_params::grammar is a raw string in llama.cpp b10200.
            // This bridge never accepts a user/output-format grammar: a non-empty
            // grammar here can only be the tool-call grammar produced by common/chat,
            // which is exactly the grammar class llama.cpp pre-fills.
            {"grammarNeedsPrefill", !params.grammar.empty()},
            {"generationPrompt", params.generation_prompt},
            {"triggerPatterns", std::move(trigger_patterns)},
            {"triggerTokens", std::move(trigger_tokens)},
            {"additionalStops", params.additional_stops},
            {"parser", params.parser},
            {"format", static_cast<int>(params.format)},
            {"reasoningFormat", parser_reasoning_format},
            {"supportsThinking", params.supports_thinking},
        };

        *out_json = dup_string(result.dump());
        return *out_json ? 0 : fail(out_error, "failed to allocate common/chat result");
    } catch (const std::exception & error) {
        return fail(out_error, error.what());
    } catch (...) {
        return fail(out_error, "unknown llama.cpp common/chat exception");
    }
}

extern "C" int og_llama_common_chat_parse(
    const char * generated,
    int format,
    int reasoning_format,
    const char * generation_prompt,
    const char * parser_source,
    char ** out_json,
    char ** out_error) {
    if (!generated || !out_json) {
        return fail(out_error, "invalid common/chat parse arguments");
    }
    *out_json = nullptr;
    if (out_error) {
        *out_error = nullptr;
    }

    try {
        common_chat_parser_params params;
        params.format = static_cast<common_chat_format>(format);
        bool strip_residual_thinking = false;
        switch (reasoning_format) {
            case 0:
                params.reasoning_format = COMMON_REASONING_FORMAT_NONE;
                break;
            case 1:
                params.reasoning_format = COMMON_REASONING_FORMAT_DEEPSEEK;
                break;
            case 2:
                // Thinking was disabled for generation. Keep reasoning parsing
                // disabled so Qwen tool-call extraction behaves exactly as before,
                // then sanitize residual template envelopes from content below.
                params.reasoning_format = COMMON_REASONING_FORMAT_NONE;
                strip_residual_thinking = true;
                break;
            default:
                return fail(out_error, "invalid common/chat reasoning format");
        }
        params.generation_prompt = generation_prompt ? generation_prompt : "";
        params.parse_tool_calls = true;
        if (parser_source && parser_source[0] != '\0') {
            params.parser.load(parser_source);
        }

        auto message = common_chat_parse(generated, false, params);
        if (strip_residual_thinking) {
            message.content = strip_think_blocks(std::move(message.content));
        }
        json tool_calls = json::array();
        for (const auto & call : message.tool_calls) {
            json arguments = json::parse(call.arguments, nullptr, false);
            if (arguments.is_discarded()) {
                return fail(out_error, "llama.cpp parsed tool call arguments that are not valid JSON");
            }
            tool_calls.push_back({
                {"id", call.id},
                {"name", call.name},
                {"arguments", std::move(arguments)},
            });
        }

        json result = {
            {"content", message.content},
            {"toolCalls", std::move(tool_calls)},
        };
        *out_json = dup_string(result.dump());
        return *out_json ? 0 : fail(out_error, "failed to allocate common/chat parse result");
    } catch (const std::exception & error) {
        return fail(out_error, error.what());
    } catch (...) {
        return fail(out_error, "unknown llama.cpp common/chat parse exception");
    }
}

extern "C" void og_llama_common_chat_string_free(char * value) {
    std::free(value);
}
