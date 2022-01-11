package com.muzimin.configuration.step

/**
 * @author: 李煌民
 * @date: 2021-12-30 18:20
 *        ${description}
 **/
case class Configuration(
                          steps: List[Step],
                          output: Option[List[Output]]
                        ) {
  override def toString: String = {
    s"""
       |Step Configuration {
       |steps: $steps,
       |output: $output
       |}
       |""".stripMargin
  }
}
