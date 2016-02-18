package com.klout.scoozie

import oozie.workflow.{ WORKFLOWu45APPOption, WORKFLOWu45APP }

/**
 * Created by nathan on 18/02/2016.
 */
package object dsl {
    type WorkflowApp = Workflow[WORKFLOWu45APP, WORKFLOWu45APPOption]
}
