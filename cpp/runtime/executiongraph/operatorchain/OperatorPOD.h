/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OPERATORPOD_H
#define OPERATORPOD_H


#include <string>
#include <vector>
#include <memory>
#include <nlohmann/json.hpp>
#include "TypeDescriptionPOD.h" // Include for TypeDescriptionPOD

namespace omnistream {
    class OperatorPOD {
    public:
    OperatorPOD() : name(""), id(""), description(""), inputs(), output(), operatorId(""), vertexID() {};

    OperatorPOD(const std::string& name, const std::string& id, const std::string& description,
        const std::vector<TypeDescriptionPOD>& inputs, const TypeDescriptionPOD& output)
        : OperatorPOD(name, id, description, inputs, output, "", 0)
    {}

    OperatorPOD(const std::string& name, const std::string& id, const std::string& description,
        const std::vector<TypeDescriptionPOD>& inputs, const TypeDescriptionPOD& output, const std::string& operatorId,
        const int vertexID) : name(name), id(id), description(description), inputs(inputs), output(output),
        operatorId(operatorId), vertexID(vertexID) {}

    OperatorPOD(const OperatorPOD& other) = default;
    OperatorPOD& operator=(const OperatorPOD& other) = default;

    std::string getName() const { return name; }
    void setName(const std::string& name) { this->name = name; }
    std::string getId() const { return id; }
    void setId(const std::string& id) { this->id = id; }
    std::string getDescription() const { return description; }
    void setDescription(const std::string& description) { this->description = description; }
    const std::vector<TypeDescriptionPOD>& getInputs() const { return inputs; }
    void setInputs(const std::vector<TypeDescriptionPOD>& inputs) { this->inputs = inputs; }
    const TypeDescriptionPOD& getOutput() const { return output; }
    void setOutput(const TypeDescriptionPOD& output) { this->output = output; }
    const int getVertexID() const { return vertexID; }
    void setVertexID(const int vertexID) { this->vertexID = vertexID; }
    std::string getOperatorId() const { return operatorId; }
    void setOperatorId(const std::string& operatorId) { this->operatorId = operatorId; }

    bool operator==(const OperatorPOD& other) const
    {
        return this->name==other.getName() && this->id==other.getId() && this->description==other.description &&
            this->inputs==other.inputs && this->output==other.output && this->vertexID==other.vertexID;
    }

    std::string toString() const
    {
        std::string inputsStr = "[";
        for (const auto& input : inputs) {
            inputsStr += input.toString() + ", ";
        }
        if (!inputs.empty()) {
            inputsStr.pop_back(); // Remove last comma and space
            inputsStr.pop_back();
        }
        inputsStr += "]";
        return "OperatorPOD{name=" + name + ", id=" + id + ", description=" + description + ", inputs=" + inputsStr +
            ", output=" + output.toString() + ", operatorId=" + operatorId + ", vertexID="
            + std::to_string(vertexID) + "}";
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(OperatorPOD, name, id, description, inputs, output, operatorId, vertexID)

    private:
        std::string name;
        std::string id;
        std::string description;
        std::vector<TypeDescriptionPOD> inputs;
        TypeDescriptionPOD output;
        std::string operatorId;
        int vertexID;
    };

} // namespace omnistream
namespace std {
  template <>
  struct hash<omnistream::OperatorPOD> {
    size_t operator()(const omnistream::OperatorPOD& OperatorPOD) const
    {
      size_t seed = 0;

      std::size_t h1 = std::hash<std::string>()(OperatorPOD.getId());
      std::size_t h2 = std::hash<std::string>()(OperatorPOD.getName());
      std::size_t h3 = std::hash<std::string>()(OperatorPOD.getDescription());
      std::vector<omnistream::TypeDescriptionPOD> v1=OperatorPOD.getInputs();

      for (const auto& elem : v1) {
        seed ^= std::hash<omnistream::TypeDescriptionPOD>{}(elem) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      }
      std::size_t h5=std::hash<omnistream::TypeDescriptionPOD>()(OperatorPOD.getOutput());
      seed ^= h1 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= h2 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= h3 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= h5 + 0x9e3779b9 + (seed << 6) + (seed >> 2);

      return seed;
    }
  };
}

#endif // OPERATORPOD_H
