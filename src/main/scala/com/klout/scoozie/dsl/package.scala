package com.klout.scoozie

import oozie.workflow_0_5.WORKFLOWu45APP
import oozie.coordinator_0_5.COORDINATORu45APP

/**
 * Types to simplify working with runners
 */
package object dsl {
    type WorkflowApp = Workflow[WORKFLOWu45APP]
    type CoordinatorApp = COORDINATORu45APP
}
