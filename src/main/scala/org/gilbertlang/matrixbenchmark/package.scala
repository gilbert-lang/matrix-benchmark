package org.gilbertlang

import org.apache.mahout.math.{VectorWritable, Vector}


package object matrixbenchmark {

  implicit def v2Writable(v: Vector): VectorWritable = new VectorWritable(v)
  implicit def vw2v(vw: VectorWritable): Vector = vw.get()
}
