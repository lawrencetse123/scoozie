/**
 * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 */

package com.klout.scoozie.dsl

import scalaxb._

sealed trait Work {
    def dependsOn(dep1: Dependency, deps: Dependency*): Node = Node(this, List(dep1) ++ deps)

    def dependsOn(deps: Seq[Dependency]): Node = Node(this, deps.toList)

    def dependsOn(sugarNode: SugarNode): SugarNode = SugarNode(this, sugarNode.dependency, Some(sugarNode))
}

case object End extends Work

trait Job[A] extends Work {
    val jobName: String
    val record: DataRecord[A]
}

case class Workflow(name: String, end: Node) extends Work

case class Kill(name: String) extends Work

sealed trait Dependency

case class ForkDependency(name: String) extends Dependency

case class JoinDependency(name: String) extends Dependency

case class Node(work: Work, dependencies: List[_ <: Dependency]) extends Dependency {

    def doIf(predicate: String) = {
        //make sure predicate string is in ${foo} format
        val Pattern =
            """[${].*[}]""".r
        val formattedPredicate = predicate match {
            case Pattern() => predicate
            case _         => "${" + predicate + "}"
        }
        val decision = Decision(formattedPredicate -> Predicates.BooleanProperty(formattedPredicate)) dependsOn dependencies
        SugarNode(work, decision option formattedPredicate)
    }

    def error = ErrorTo(this) //used to set a custom error-to on a node
}

case object Start extends Dependency

case class OneOf(dep1: Dependency, deps: Dependency*) extends Dependency

case class SugarNode(work: Work, dependency: DecisionDependency, previousSugarNode: Option[SugarNode] = None)

object Optional {
    def toNode(sugarNode: SugarNode): Node = sugarNode.previousSugarNode match {
        case Some(previous) => Node(sugarNode.work, List(toNode(previous)))
        case _              => Node(sugarNode.work, List(sugarNode.dependency))
    }

    def apply(sugarNode: SugarNode) = OneOf(sugarNode.dependency.parent default, toNode(sugarNode))
}

sealed trait Predicate

object Predicates {

    case object AlwaysTrue extends Predicate

    case class BooleanProperty(property: String) extends Predicate {
        val BooleanPropertyRegex = """\$\{(.*)\}""" r

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
