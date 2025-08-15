/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/12/25.
//

#ifndef ABSTRACTIDPOD_H
#define ABSTRACTIDPOD_H

#include <iostream>
#include <string>
#include <nlohmann/json.hpp>

namespace omnistream {

class AbstractIDPOD {
public:
  // Default constructor
  AbstractIDPOD() : upperPart(0), lowerPart(0) {}

  // Full argument constructor
  AbstractIDPOD(long upper, long lower) : upperPart(upper), lowerPart(lower) {}

  AbstractIDPOD(const AbstractIDPOD& other)
    : upperPart(other.upperPart),
      lowerPart(other.lowerPart)
  {
  }

  AbstractIDPOD(AbstractIDPOD&& other) noexcept
    : upperPart(other.upperPart),
      lowerPart(other.lowerPart)
  {
  }

  AbstractIDPOD& operator=(const AbstractIDPOD& other)
  {
    if (this == &other) {
      return *this;
    }
    upperPart = other.upperPart;
    lowerPart = other.lowerPart;
    return *this;
  }

  AbstractIDPOD& operator=(AbstractIDPOD&& other) noexcept
  {
    if (this == &other) {
      return *this;
    }
    upperPart = other.upperPart;
    lowerPart = other.lowerPart;
    return *this;
  }

  // Getters
  virtual long getUpperPart() const { return upperPart; }
  virtual long getLowerPart() const { return lowerPart; }

  // Setters
  virtual void setUpperPart(long upper) { upperPart = upper; }
  virtual void setLowerPart(long lower) { lowerPart = lower; }

  // toString method
  virtual std::string toString() const
  {
    return std::to_string(upperPart) + "-" + std::to_string(lowerPart);
  }

  friend bool operator==(const AbstractIDPOD& lhs, const AbstractIDPOD& rhs)
  {
    return lhs.upperPart == rhs.upperPart
      && lhs.lowerPart == rhs.lowerPart;
  }

  friend bool operator!=(const AbstractIDPOD& lhs, const AbstractIDPOD& rhs)
  {
    return !(lhs == rhs);
  }

  friend std::size_t hash_value(const AbstractIDPOD& obj)
  {
    std::size_t seed = 0x358D0F80;
    seed ^= (seed << 6) + (seed >> 2) + 0x105088E5 + static_cast<std::size_t>(obj.upperPart);
    seed ^= (seed << 6) + (seed >> 2) + 0x291D5EEC + static_cast<std::size_t>(obj.lowerPart);
    return seed;
  }

  //friend std::ostream& operator<<(std::ostream& os, const AbstractIDPOD& obj);

  NLOHMANN_DEFINE_TYPE_INTRUSIVE(AbstractIDPOD, upperPart, lowerPart)
protected:
  long upperPart;
  long lowerPart;
};


} // namespace omnistream


namespace std {
  template <>
  struct hash<omnistream::AbstractIDPOD> {
    size_t operator()(const omnistream::AbstractIDPOD& key) const
    {
      return hash_value(key);
    }
  };
}
#endif // ABSTRACTIDPOD_H
