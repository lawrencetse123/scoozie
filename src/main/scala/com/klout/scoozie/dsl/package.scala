package com.klout.scoozie

import oozie.workflow.{ WORKFLOWu45APPOption, WORKFLOWu45APP }
import oozie.coordinator.COORDINATORu45APP

/**
 * Types to simplify working with runners
 */
package object dsl {
    type WorkflowApp = Workflow[WORKFLOWu45APP, WORKFLOWu45APPOption]
    type CoordinatorApp = COORDINATORu45APP
}
