/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.expressions;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import java.util.Arrays;
import java.util.Objects;

/**
 * The interface of a function expression. A function expression is an expression that takes a
 * function name and a list of arguments.
 */
@Evolving
public interface FunctionExpression extends Expression {

  /**
   * Creates a new {@link FunctionExpression} with the given function name and arguments.
   *
   * @param functionName The name of the function
   * @param arguments The arguments to the function
   * @return The created {@link FunctionExpression}
   */
  static FuncExpressionImpl of(String functionName, Expression... arguments) {
    return new FuncExpressionImpl(functionName, arguments);
  }

  /**
   * Creates a new {@link FunctionExpression} with the given function name and no arguments.
   *
   * @param functionName The name of the function
   * @return The created {@link FunctionExpression}
   */
  static FuncExpressionImpl of(String functionName) {
    return of(functionName, Expression.EMPTY_EXPRESSION);
  }

  /** @return The transform function name. */
  String functionName();

  /** @return The arguments passed to the transform function. */
  Expression[] arguments();

  @Override
  default Expression[] children() {
    return arguments();
  }

  /** A {@link FunctionExpression} implementation */
  final class FuncExpressionImpl implements FunctionExpression {
    private final String functionName;
    private final Expression[] arguments;

    private FuncExpressionImpl(String functionName, Expression[] arguments) {
      this.functionName = functionName;
      this.arguments = arguments;
    }

    @Override
    public String functionName() {
      return functionName;
    }

    @Override
    public Expression[] arguments() {
      return arguments;
    }

    /** @return The string representation of the function expression. */
    @Override
    public String toString() {
      if (arguments.length == 0) {
        return functionName + "()";
      }
      String[] functionArguments =
          Arrays.stream(this.arguments)
              .map(
                  expression -> {
                    if (expression instanceof Literals.LiteralImpl) {
                      return ((Literals.LiteralImpl<?>) expression).value().toString();
                    }
                    return expression.toString();
                  })
              .toArray(String[]::new);
      return functionName + "(" + String.join(", ", functionArguments) + ")";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FuncExpressionImpl that = (FuncExpressionImpl) o;
      return Objects.equals(functionName, that.functionName)
          && Arrays.equals(arguments, that.arguments);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(functionName);
      result = 31 * result + Arrays.hashCode(arguments);
      return result;
    }
  }
}
