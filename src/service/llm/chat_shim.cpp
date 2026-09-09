#include "chat.h"
#include "common.h"

#include <nlohmann/json.hpp>

#include <cstdlib>
#include <cstring>
#include <exception>
#include <string>

using json = nlohmann::ordered_json;

namespace {

char * dup_string(const std::string & value) {
    auto * out = static_cast<char *>(std::malloc(value.size() + 1));
    if (out == nullptr) {
        return nullptr;
    }
    std::memcpy(out, value.data(), value.size());
    out[value.size()] = '\0';
    return out;
}

int write_json(const json & value, char ** out_json) {
    *out_json = dup_string(value.dump());
    return *out_json == nullptr ? -2 : 0;
}

int write_error(const std::string & message, char ** out_json) {
    if (out_json != nullptr) {
        *out_json = dup_string(json{{"error", message}}.dump());
    }
    return -1;
}

common_chat_msg parse_message(const json & value) {
    common_chat_msg message;
    message.role = value.at("role").get<std::string>();
    message.content = value.value("content", std::string{});
    message.tool_call_id = value.value("toolCallId", std::string{});
    message.tool_name = value.value("name", std::string{});

    if (value.contains("toolCalls")) {
        for (const auto & call : value.at("toolCalls")) {
            common_chat_tool_call tool_call;
            tool_call.id = call.value("id", std::string{});
            tool_call.name = call.at("name").get<std::string>();
            tool_call.arguments = call.at("arguments").dump();
            message.tool_calls.push_back(std::move(tool_call));
        }
    }
    return message;
}

common_chat_tool parse_tool(const json & value) {
    common_chat_tool tool;
    tool.name = value.at("name").get<std::string>();
    tool.description = value.value("description", std::string{});
    tool.parameters = value.at("parameters").dump();
    return tool;
}

json tool_arguments(const std::string & value) {
    try {
        return json::parse(value);
    } catch (...) {
        return value;
    }
}

} // namespace

extern "C" int og_llama_chat_apply(
    const llama_model * model,
    const char * chat_template,
    const char * messages_json,
    const char * tools_json,
    char ** out_json) {
    if (model == nullptr || chat_template == nullptr || messages_json == nullptr ||
        tools_json == nullptr || out_json == nullptr) {
        return write_error("invalid native chat arguments", out_json);
    }
    *out_json = nullptr;

    try {
        const auto messages = json::parse(messages_json);
        const auto tools = json::parse(tools_json);

        common_chat_templates_inputs inputs;
        inputs.add_generation_prompt = true;
        inputs.use_jinja = true;
        inputs.tool_choice = COMMON_CHAT_TOOL_CHOICE_AUTO;
        inputs.parallel_tool_calls = false;

        for (const auto & value : messages) {
            inputs.messages.push_back(parse_message(value));
        }
        for (const auto & value : tools) {
            inputs.tools.push_back(parse_tool(value));
        }

        auto templates = common_chat_templates_init(model, chat_template);
        if (!templates) {
            return write_error("llama.cpp could not initialize the chat template", out_json);
        }
        const auto params = common_chat_templates_apply(templates.get(), inputs);

        json trigger_patterns = json::array();
        json trigger_tokens = json::array();
        for (const auto & trigger : params.grammar_triggers) {
            switch (trigger.type) {
                case COMMON_GRAMMAR_TRIGGER_TYPE_WORD:
                    trigger_patterns.push_back(regex_escape(trigger.value));
                    break;
                case COMMON_GRAMMAR_TRIGGER_TYPE_PATTERN:
                    trigger_patterns.push_back(trigger.value);
                    break;
                case COMMON_GRAMMAR_TRIGGER_TYPE_PATTERN_FULL: {
                    std::string anchored = "^$";
                    if (!trigger.value.empty()) {
                        anchored = (trigger.value.front() == '^' ? "" : "^") + trigger.value +
                            (trigger.value.back() == '$' ? "" : "$");
                    }
                    trigger_patterns.push_back(anchored);
                    break;
                }
                case COMMON_GRAMMAR_TRIGGER_TYPE_TOKEN:
                    trigger_tokens.push_back(trigger.token);
                    break;
                default:
                    return write_error("llama.cpp returned an unknown grammar trigger type", out_json);
            }
        }

        return write_json(json{
            {"prompt", params.prompt},
            {"grammar", params.grammar},
            {"grammarLazy", params.grammar_lazy},
            {"generationPrompt", params.generation_prompt},
            {"format", static_cast<int>(params.format)},
            {"parser", params.parser},
            {"triggerPatterns", std::move(trigger_patterns)},
            {"triggerTokens", std::move(trigger_tokens)},
            {"additionalStops", params.additional_stops},
        }, out_json);
    } catch (const std::exception & error) {
        return write_error(error.what(), out_json);
    } catch (...) {
        return write_error("unknown llama.cpp chat error", out_json);
    }
}

extern "C" int og_llama_chat_parse(
    const char * generated,
    int format,
    const char * generation_prompt,
    const char * parser,
    char ** out_json) {
    if (generated == nullptr || generation_prompt == nullptr || parser == nullptr || out_json == nullptr) {
        return write_error("invalid native chat parse arguments", out_json);
    }
    *out_json = nullptr;

    try {
        common_chat_parser_params params;
        params.format = static_cast<common_chat_format>(format);
        params.generation_prompt = generation_prompt;
        params.parse_tool_calls = true;
        if (parser[0] != '\0') {
            params.parser.load(parser);
        }

        const auto message = common_chat_parse(generated, false, params);
        json calls = json::array();
        std::size_t index = 0;
        for (const auto & call : message.tool_calls) {
            const auto id = call.id.empty() ? "call_" + std::to_string(index) : call.id;
            calls.push_back(json{
                {"id", id},
                {"name", call.name},
                {"arguments", tool_arguments(call.arguments)},
            });
            ++index;
        }

        return write_json(json{
            {"content", message.content},
            {"toolCalls", std::move(calls)},
        }, out_json);
    } catch (const std::exception & error) {
        return write_error(error.what(), out_json);
    } catch (...) {
        return write_error("unknown llama.cpp chat parse error", out_json);
    }
}

extern "C" void og_llama_chat_free(char * value) {
    std::free(value);
}
