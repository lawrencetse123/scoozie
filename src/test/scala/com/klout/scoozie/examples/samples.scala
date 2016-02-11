/**
 * Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 */

package com.klout.scoozie.examples

import com.klout.scoozie.dsl._
import com.klout.scoozie.jobs.{ MkDir, FsJob, MapReduceJob, NoOpJob }
import com.klout.scoozie.workflow.WorkflowImpl

object DecisionSamples {

    val FirstJob = MapReduceJob("foo") dependsOn Start

    val SomeDecision = Decision(
        "ifBing" -> Predicates.BooleanProperty("ifBing")
    ) dependsOn Start

    val Route1 = MapReduceJob("skippingBing") dependsOn (SomeDecision default)
    val Route2 = MapReduceJob("processBing") dependsOn (SomeDecision option "ifBing")
    val Done = End dependsOn OneOf(Route1, Route2)

    val Pipeline = WorkflowImpl("decisions", Done)

    def newDecisionSample = {
        val first = NoOpJob("first") dependsOn Start
        val optionalNode = NoOpJob("optional") dependsOn first doIf "${doOptionalNode}"
        val alwaysDo = NoOpJob("always do") dependsOn Optional(optionalNode)
        val optionalNode2 = {
            val sub1 = NoOpJob("sub1") dependsOn Start
            val sub2 = NoOpJob("sub2") dependsOn sub1
            val sub3 = NoOpJob("sub3") dependsOn sub2
            WorkflowImpl("sub-wf", sub3)
        } dependsOn alwaysDo doIf "{doSubWf}"
        val alwaysDo2 = NoOpJob("always do 2") dependsOn Optional(optionalNode2)
        val end = End dependsOn alwaysDo2
        WorkflowImpl("new-decision", end)
    }
}

object SimpleSamples {
    def EmptyWorkflow = {
        val end = End dependsOn Nil
        WorkflowImpl("empty", end)
    }

    def SingleWorkflow = {
        val start = MapReduceJob("start") dependsOn Start
        val end = End dependsOn start
        WorkflowImpl("single", end)
    }

    def HelloWorldWorkflow = {
        val mkHelloWorld = FsJob(
            name = "make-hello-world",
            tasks = List(MkDir("${nameNode}/users/test/mj/oozie-fun/hello-world_${wf:id()}"))
        ) dependsOn Start
        val done = End dependsOn mkHelloWorld
        WorkflowImpl("hello-world-wf", done)
    }

    def SimpleWorkflow = {
        val first = MapReduceJob("first") dependsOn Start
        val second = MapReduceJob("second") dependsOn first
        val third = MapReduceJob("third") dependsOn second
        val fourth = MapReduceJob("fourth") dependsOn third
        val end = End dependsOn fourth
        WorkflowImpl("simple", end)
    }

    def SimpleForkJoin = {
        val first = MapReduceJob("first") dependsOn Start
        val secondA = MapReduceJob("secondA") dependsOn first
        val secondB = MapReduceJob("secondB") dependsOn first
        val end = End dependsOn (secondA, secondB)
        WorkflowImpl("simple-fork-join", end)
    }

    def SimpleDecision = {
        val first = MapReduceJob("first") dependsOn Start
        val decision = Decision("route1" -> Predicates.AlwaysTrue) dependsOn first //decision is a DecisionNode
        val default = MapReduceJob("default") dependsOn (decision default)
        val option = MapReduceJob("option") dependsOn (decision option "route1")
        val second = MapReduceJob("second") dependsOn OneOf(default, option)
        val done = End dependsOn second
        WorkflowImpl("simple-decision", done)
    }

    def SimpleSubWorkflow = {
        val first = MapReduceJob("begin") dependsOn Start
        val subWf = SimpleWorkflow dependsOn first
        val third = MapReduceJob("final") dependsOn subWf
        val end = End dependsOn third
        WorkflowImpl("simple-sub-workflow", end)
    }

    def TwoSimpleForkJoins = {
        val first = MapReduceJob("first") dependsOn Start
        val secondA = MapReduceJob("secondA") dependsOn first
        val secondB = MapReduceJob("secondB") dependsOn first
        val third = MapReduceJob("third") dependsOn (secondA, secondB)
        val fourthA = MapReduceJob("fourthA") dependsOn third
        val fourthB = MapReduceJob("fourthB") dependsOn third
        val end = End dependsOn (fourthA, fourthB)
        WorkflowImpl("two-simple-fork-joins", end)
    }

    /* Not allowed by Oozie */
    def NestedForkJoin = {
        val first = MapReduceJob("first") dependsOn Start
        val secondA = MapReduceJob("secondA") dependsOn first
        val secondB = MapReduceJob("secondB") dependsOn first
        val thirdA = MapReduceJob("thirdA") dependsOn secondA
        val thirdB = MapReduceJob("thirdB") dependsOn secondA
        val thirdC = MapReduceJob("thirdC") dependsOn secondB
        val fourth = MapReduceJob("fourth") dependsOn (thirdA, thirdB, thirdC)
        WorkflowImpl("nested-fork-join", fourth)
    }

    /* Not allowed by Oozie */
    def NestedForkJoinFs = {
        val root = "${nameNode}/users/test/mj/oozie-fun"
        val first = FsJob(
            name = "firstFs",
            tasks = List(MkDir(s"$root/firstDir"))
        ) dependsOn Start
        val secondA = FsJob(
            name = "secondFsA",
            tasks = List(MkDir(s"$root/firstDir/secondDirA"))
        ) dependsOn first
        val secondB = FsJob(
            name = "secondFsB",
            tasks = List(MkDir(s"$root/firstDir/secondDirB"))
        ) dependsOn first
        val thirdA = FsJob(
            name = "thirdFsA",
            tasks = List(MkDir(s"$root/firstDir/secondDirA/thirdDirA"))
        ) dependsOn secondA
        val thirdB = FsJob(
            name = "thirdFsB",
            tasks = List(MkDir(s"$root/firstDir/secondDirA/thirdDirB"))
        ) dependsOn secondA
        val thirdC = FsJob(
            name = "thirdFsC",
            tasks = List(MkDir(s"$root/firstDir/secondDirB/thirdDirC"))
        ) dependsOn secondB
        val fourth = FsJob(
            name = "fourth",
            tasks = List(
                MkDir(s"$root/firstDir/secondDirA/thirdDirA/fourthDir"),
                MkDir(s"$root/firstDir/secondDirA/thirdDirB/fourthDir"),
                MkDir(s"$root/firstDir/secondDirB/thirdDirC/fourthDir"))
        ) dependsOn (thirdA, thirdB, thirdC)
        val end = End dependsOn fourth
        WorkflowImpl("test-nested-fork-join", end)
    }

    /* Allowed by Oozie */
    def NestedForkJoinFs2 = {
        val root = "${nameNode}/users/test/mj/oozie-fun"
        val first = FsJob(
            name = "firstFs",
            tasks = List(MkDir(s"$root/firstDir"))
        ) dependsOn Start
        //fork-secondA-secondB
        val secondA = FsJob(
            name = "secondFsA",
            tasks = List(MkDir(s"$root/firstDir/secondDirA"))
        ) dependsOn first
        val secondB = FsJob(
            name = "secondFsB",
            tasks = List(MkDir(s"$root/firstDir/secondDirB"))
        ) dependsOn first
        //fork-thirdA-thirdB
        val thirdA = FsJob(
            name = "thirdFsA",
            tasks = List(MkDir(s"$root/firstDir/secondDirA/thirdDirA"))
        ) dependsOn secondA
        val thirdB = FsJob(
            name = "thirdFsB",
            tasks = List(MkDir(s"$root/firstDir/secondDirA/thirdDirB"))
        ) dependsOn secondA
        //join-thirdA-thirdB
        val fourth = FsJob(
            name = "fourth",
            tasks = List(
                MkDir(s"$root/firstDir/secondDirA/thirdDirA/fourthDir"),
                MkDir(s"$root/firstDir/secondDirA/thirdDirB/fourthDir"))
        ) dependsOn (thirdA, thirdB)
        //join-fourth(secondA)-secondB
        val last = FsJob(
            name = "last",
            tasks = List(
                MkDir(s"$root/firstDir/secondDirA/thirdDirA/fourthDir/fifthDir"),
                MkDir(s"$root/firstDir/secondDirA/thirdDirB/fourthDir/fifthDir"),
                MkDir(s"$root/firstDir/secondDirB/fifthDir"))
        ) dependsOn (fourth, secondB)
        val end = End dependsOn last
        WorkflowImpl("test-nested-fork-join-2", end)
    }

    /* Not allowed by Oozie */
    def NestedForkJoinFs3 = {
        val root = "${nameNode}/users/test/mj/oozie-fun"
        val first = FsJob(
            name = "firstFs",
            tasks = List(MkDir(s"$root/firstDir"))
        ) dependsOn Start
        //fork-secondA-secondB
        val secondA = FsJob(
            name = "secondFsA",
            tasks = List(MkDir(s"$root/firstDir/secondDirA"))
        ) dependsOn first
        val secondB = FsJob(
            name = "secondFsB",
            tasks = List(MkDir(s"$root/firstDir/secondDirB"))
        ) dependsOn first
        //fork-thirdA-thirdB
        val thirdA = FsJob(
            name = "thirdFsA",
            tasks = List(MkDir(s"$root/firstDir/secondDirA/thirdDirA"))
        ) dependsOn secondA
        val thirdB = FsJob(
            name = "thirdFsB",
            tasks = List(MkDir(s"$root/firstDir/secondDirA/thirdDirB"))
        ) dependsOn secondA
        //fork-thirdC-thirdD
        val thirdC = FsJob(
            name = "thirdFsC",
            tasks = List(MkDir(s"$root/firstDir/secondDirB/thirdDirC"))
        ) dependsOn secondB
        val thirdD = FsJob(
            name = "thirdFsD",
            tasks = List(MkDir(s"$root/firstDir/secondDirB/thirdDirD"))
        ) dependsOn secondB
        //join
        val end = End dependsOn (thirdA, thirdB, thirdC, thirdD)
        WorkflowImpl("test-nested-fork-join-3", end)
    }

    /* Not allowed by Oozie */
    def NestedForkJoinFs4 = {
        val root = "${nameNode}/users/test/mj/oozie-fun"
        val first = FsJob(
            name = "firstFs",
            tasks = List(MkDir(s"$root/firstDir"))
        ) dependsOn Start
        //fork-secondA-secondB
        val secondA = FsJob(
            name = "secondFsA",
            tasks = List(MkDir(s"$root/firstDir/secondDirA"))
        ) dependsOn first
        val secondB = FsJob(
            name = "secondFsB",
            tasks = List(MkDir(s"$root/firstDir/secondDirB"))
        ) dependsOn first
        //fork-thirdA-thirdB
        val thirdA = FsJob(
            name = "thirdFsA",
            tasks = List(MkDir(s"$root/firstDir/secondDirA/thirdDirA"))
        ) dependsOn secondA
        val thirdB = FsJob(
            name = "thirdFsB",
            tasks = List(MkDir(s"$root/firstDir/secondDirA/thirdDirB"))
        ) dependsOn secondA
        //join-thirdB-secondB
        val fourth = FsJob(
            name = "fourth",
            tasks = List(
                MkDir(s"$root/firstDir/secondDirA/thirdDirB/fourthDir"),
                MkDir(s"$root/firstDir/secondDirB/fourthDir"))
        ) dependsOn (thirdB, secondB)
        //join-thirdA-fourth
        val end = End dependsOn (thirdA, fourth)
        WorkflowImpl("test-nested-fork-join-4", end)
    }

    def SubworkflowWithForkJoins = {
        val start = MapReduceJob("start") dependsOn Start
        val sub = SimpleWorkflow dependsOn start
        val thirdA = MapReduceJob("thirdA") dependsOn sub
        val thirdB = MapReduceJob("thirdB") dependsOn sub
        val end = End dependsOn (thirdA, thirdB)
        WorkflowImpl("sub-fork-join", end)
    }

    val TestSubWorkflow = {
        val first = MapReduceJob("start") dependsOn Start
        val sub1 = SimpleWorkflow dependsOn first
        val middle = MapReduceJob("middle") dependsOn sub1
        val sub2 = SimpleWorkflow dependsOn middle
        val last = MapReduceJob("last") dependsOn sub2
        val end = End dependsOn last
        WorkflowImpl("test-sub-wf", end)
    }

    val DuplicateSubWorkflows = {
        val begin = MapReduceJob("begin") dependsOn Start
        val sub1 = SimpleWorkflow dependsOn begin
        val middle = MapReduceJob("middle") dependsOn sub1
        val sub2 = SimpleWorkflow dependsOn middle
        val end = End dependsOn sub2
        WorkflowImpl("duplicate-sub-workflows", end)
    }

    def CustomErrorTo = {
        val first = MapReduceJob("first") dependsOn Start
        val errorPath = MapReduceJob("error") dependsOn (first error)
        val second = MapReduceJob("second") dependsOn first
        val end = End dependsOn OneOf(second, errorPath)
    }

    def SugarOption = {
        val first = MapReduceJob("first") dependsOn Start
        val option = MapReduceJob("option") dependsOn first doIf "doOption"
        val second = MapReduceJob("second") dependsOn Optional(option)
        val done = End dependsOn second
        WorkflowImpl("sugar-option-decision", done)
    }

    def SubWfExample = {
        val begin = MapReduceJob("begin") dependsOn Start
        val someWork = MapReduceJob("someWork") dependsOn begin
        val subwf = SugarOption dependsOn someWork
        val end = End dependsOn subwf
        WorkflowImpl("sub-wf-example", end)
    }

    def DecisionExample = {
        val first = MapReduceJob("first") dependsOn Start
        val decision = Decision(
            "route1" -> Predicates.BooleanProperty("${doRoute1}")
        ) dependsOn first
        val route1Start = MapReduceJob("r1Start") dependsOn (decision option "route1")
        val route1End = MapReduceJob("r1End") dependsOn route1Start
        val route2Start = MapReduceJob("r2Start") dependsOn (decision default)
        val route2End = MapReduceJob("r2End") dependsOn route2Start
        val last = MapReduceJob("last") dependsOn OneOf(route1End, route2End)
        val done = End dependsOn last
        WorkflowImpl("decision-example", done)
    }
}
