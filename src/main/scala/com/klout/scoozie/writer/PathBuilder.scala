package com.klout.scoozie.writer

import com.klout.scoozie.ScoozieConfig._

class PathBuilder(rootFolderPath: String) {

  def getTargetFolderPath: String = s"$rootFolderPath"

  def getWorkflowFolderPath: String = s"$getTargetFolderPath/$workflowFolderName"

  def getCoordinatorFolderPath: String = s"$getTargetFolderPath/$coordinatorFolderName"

  def getBundleFolderPath: String = s"$getTargetFolderPath/$bundleFolderName"

  def getPropertiesFilePath: String = s"$getTargetFolderPath/$propertyFileName"

  def getWorkflowFilePath(workflowFileName: String): String = s"$getWorkflowFolderPath/$workflowFileName"

  def getCoordinatorFilePath(coordinatorFileName: String): String = s"$getCoordinatorFolderPath/$coordinatorFileName"

  def getBundleFilePath(bundleFileName: String): String = s"$getBundleFolderPath/$bundleFileName"
}