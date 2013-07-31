/**
 * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 */

package com.klout.scoozie
package dsl

sealed trait Work {
    def dependsOn(dep1: Dependency, deps: Dependency*): Node = Node(this, List(dep1) ++ deps)
    def dependsOn(deps: Seq[Dependency]): Node = Node(this, deps.toList)
}

case object End extends Work

trait Job extends Work {
    val jobName: String
}

case class Workflow(name: String, end: Node) extends Work

case class Kill(name: String) extends Work

sealed trait Dependency {

    //for now: only supports single optional dependency
    def andOptionally_:(dep: Dependency) = SugarOption(dep, List(this): _*)
}

case class ForkDependency(name: String) extends Dependency

case class JoinDependency(name: String) extends Dependency

case class Node(work: Work, dependencies: List[_ <: Dependency]) extends Dependency {

    /*
     * used when a route is optional (i.e. decision -> optional route -> default) 
     * simplified to route doIf "predicate", default dependsOn parent andOptionally route
     */
    def doIf(predicate: String) = {
        //make sure predicate string is in ${foo} format
        val Pattern = """[${].*[}]""".r
        val formattedPredicate = predicate match {
            case Pattern() => predicate
            case _         => "${" + predicate + "}"
        }
        Node(work, List(DoIf(formattedPredicate, dependencies.toSeq: _*)))
    }

    def error = ErrorTo(this) //used to set a custom error-to on a node
}

case object Start extends Dependency

case class OneOf(dep1: Dependency, deps: Dependency*) extends Dependency

case class SugarOption(required: Dependency, optional: Dependency*) extends Dependency

sealed trait Predicate

object Predicates {
    case object AlwaysTrue extends Predicate

    case class BooleanProperty(property: String) extends Predicate {
        val BooleanPropertyRegex = """\$\{(.*)\}"""r

        lazy val formattedProperty = property match {
            case BooleanPropertyRegex(_) => property
            case _                       => """${%s}""" format property
        }
    }
}

case class Decision(predicates: List[(String, Predicate)]) {
    def dependsOn(dep1: Dependency, deps: Dependency*): DecisionNode = DecisionNode(this, Set(dep1) ++ deps)
    def dependsOn(deps: Seq[Dependency]): DecisionNode = DecisionNode(this, deps.toSet)
}

object Decision {
    def apply(pair1: (String, Predicate), pairs: (String, Predicate)*): Decision = Decision(pair1 :: pairs.toList)
}

case class DecisionDependency(parent: DecisionNode, option: Option[String]) extends Dependency

case class DecisionNode(decision: Decision, dependencies: Set[_ <: Dependency]) extends Dependency {
    val default: Dependency = DecisionDependency(this, None)
    val option: String => DecisionDependency = name => DecisionDependency(this, Some(name))

}

case class DoIf(predicate: String, deps: Dependency*) extends Dependency

case class ErrorTo(node: Node) extends Dependency
