package com.muzimin.configuration.job.config

import com.muzimin.input.Reader

/**
 * @author: 李煌民
 * @date: 2021-12-13 11:03
 *        ${description}
 **/
trait InputConfig {
  def getReader(name: String): Reader
}
