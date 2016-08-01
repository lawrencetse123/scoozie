package com.klout.scoozie.runner

import org.specs2.specification.BeforeAfterAll

trait BeforeAfterAllStackable extends BeforeAfterAll {
  def beforeAll(): Unit = ()
  def afterAll(): Unit = ()
}
