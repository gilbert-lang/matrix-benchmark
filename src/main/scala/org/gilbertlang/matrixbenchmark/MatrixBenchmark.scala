package org.gilbertlang.matrixbenchmark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.serializer.KryoSerializer
import org.apache.mahout.math.{DenseVector, SequentialAccessSparseVector, RandomAccessSparseVector}

import scala.transient
import org.apache.spark.rdd.RDD

import org.apache.mahout.math.function.Functions

import scala.collection.JavaConversions._
import java.util

object RunBenchmark {
  def main(args: Array[String]): Unit = {

//    val pathToMatrix = "/home/ssc/Entwicklung/datasets/movielens1M/movielens.csv"
//    val numColumns = 3952

//    val pathToMatrix = "/home/ssc/Entwicklung/datasets/movielens10M/movielens.csv"
//    val numColumns = 65133

    val pathToMatrix = "/home/ssc/Entwicklung/datasets/slashdotzoo/slashdot.csv"
    val numColumns = 79120

    val sparkMaster = "local"
    val degreeOfParallelism = 4

//    val benchmark = new MatrixBenchmark(sparkMaster, pathToMatrix, numColumns, degreeOfParallelism)
//
//    val denseCells = MatrixBenchmark.generateVectorEntries(numColumns, 1.0, false)
//    val sparseCells = MatrixBenchmark.generateVectorEntries(numColumns, 0.1, false)
//    val superSparseCells = MatrixBenchmark.generateVectorEntries(numColumns, 0.01, false)
//
//    val denseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 1.0, true)
//    val sparseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 0.1, true)
//    val superSparseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 0.01, true)
//
//    val buffer = new StringBuilder("---------------------------\nRESULTS\n---------------------------\n\n")
//    buffer.append("cell-cell-dense " + benchmark.cellTimesCell(denseCells) + "ms\n")
//    buffer.append("cell-cell-sparse " + benchmark.cellTimesCell(sparseCells) + "ms\n")
//    buffer.append("cell-cell-supersparse " + benchmark.cellTimesCell(superSparseCells) + "ms\n")
//    buffer.append("\n")
//    buffer.append("row-column-dense " + benchmark.rowTimesColumn(denseCellsZero, true) + "ms\n")
//    buffer.append("row-column-sparse " + benchmark.rowTimesColumn(sparseCellsZero, false) + "ms\n")
//    buffer.append("row-column-supersparse " + benchmark.rowTimesColumn(superSparseCellsZero, false) + "ms\n")
//    buffer.append("\n")
//    buffer.append("row-cell-dense " + benchmark.rowTimesCell(denseCellsZero) + "ms\n")
//    buffer.append("row-cell-sparse " + benchmark.rowTimesCell(sparseCellsZero) + "ms\n")
//    buffer.append("row-cell-supersparse " + benchmark.rowTimesCell(superSparseCellsZero) + "ms\n")
//    buffer.append("\n")
//    buffer.append("column-cell-dense " + benchmark.columnTimesCell(denseCellsZero) + "ms\n")
//    buffer.append("column-cell-sparse " + benchmark.columnTimesCell(sparseCellsZero) + "ms\n")
//    buffer.append("column-cell-supersparse " + benchmark.columnTimesCell(superSparseCellsZero) + "ms\n")
//    buffer.append("\n")
//    buffer.append("column-column-dense " + benchmark.columnTimesCell(denseCellsZero) + "ms\n")
//    buffer.append("column-column-sparse " + benchmark.columnTimesCell(sparseCellsZero) + "ms\n")
//    buffer.append("column-column-supersparse " + benchmark.columnTimesCell(superSparseCellsZero) + "ms\n")
//    buffer.append("\n")
//    buffer.append("cell-column-dense " + benchmark.cellTimesColumn(denseCellsZero, true) + "ms\n")
//    buffer.append("cell-column-sparse " + benchmark.cellTimesColumn(sparseCellsZero, false) + "ms\n")
//    buffer.append("cell-column-supersparse " + benchmark.cellTimesColumn(superSparseCellsZero, false) + "ms\n")
//    println(buffer)

    val runner = new MatrixBenchmarkRunner(sparkMaster, pathToMatrix, numColumns, degreeOfParallelism)

    val buffer = new StringBuilder("---------------------------\nRESULTS\n---------------------------\n\n")

    for (_ <- 1 to 3) {
      runner.runCellCell(buffer)
      runner.runRowColumn(buffer)
      runner.runRowCell(buffer)
      runner.runColumnCell(buffer)
      //runner.runColumnColumn(buffer)
      //runner.runCellColumn(buffer)
    }

    println(buffer)
  }



}

class MatrixBenchmarkRunner(sparkMaster: String, pathToMatrix: String, numColumns: Int, degreeOfParallelism: Int) {

  def runCellCell(buffer: StringBuilder) = {
    val benchmark = new MatrixBenchmark(sparkMaster, pathToMatrix, numColumns, degreeOfParallelism)

    val denseCells = MatrixBenchmark.generateVectorEntries(numColumns, 1.0, false)
    val sparseCells = MatrixBenchmark.generateVectorEntries(numColumns, 0.1, false)
    val superSparseCells = MatrixBenchmark.generateVectorEntries(numColumns, 0.01, false)

    buffer.append("cell-cell-dense " + benchmark.cellTimesCell(denseCells) + "ms\n")
    buffer.append("cell-cell-sparse " + benchmark.cellTimesCell(sparseCells) + "ms\n")
    buffer.append("cell-cell-supersparse " + benchmark.cellTimesCell(superSparseCells) + "ms\n")
    buffer.append("\n")

    benchmark.shutdown()
  }

  def runRowColumn(buffer: StringBuilder) = {

    val benchmark = new MatrixBenchmark(sparkMaster, pathToMatrix, numColumns, degreeOfParallelism)

    val denseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 1.0, true)
    val sparseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 0.1, true)
    val superSparseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 0.01, true)

    buffer.append("row-column-dense " + benchmark.rowTimesColumn(denseCellsZero, true) + "ms\n")
    buffer.append("row-column-sparse " + benchmark.rowTimesColumn(sparseCellsZero, false) + "ms\n")
    buffer.append("row-column-supersparse " + benchmark.rowTimesColumn(superSparseCellsZero, false) + "ms\n")
    buffer.append("\n")

    benchmark.shutdown()
  }

  def runRowCell(buffer: StringBuilder) = {

    val benchmark = new MatrixBenchmark(sparkMaster, pathToMatrix, numColumns, degreeOfParallelism)

    val denseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 1.0, true)
    val sparseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 0.1, true)
    val superSparseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 0.01, true)

    buffer.append("row-cell-dense " + benchmark.rowTimesCell(denseCellsZero) + "ms\n")
    buffer.append("row-cell-sparse " + benchmark.rowTimesCell(sparseCellsZero) + "ms\n")
    buffer.append("row-cell-supersparse " + benchmark.rowTimesCell(superSparseCellsZero) + "ms\n")
    buffer.append("\n")

    benchmark.shutdown()
  }

  def runColumnCell(buffer: StringBuilder) = {

    val benchmark = new MatrixBenchmark(sparkMaster, pathToMatrix, numColumns, degreeOfParallelism)

    val denseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 1.0, true)
    val sparseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 0.1, true)
    val superSparseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 0.01, true)

    buffer.append("column-cell-dense " + benchmark.columnTimesCell(denseCellsZero) + "ms\n")
    buffer.append("column-cell-sparse " + benchmark.columnTimesCell(sparseCellsZero) + "ms\n")
    buffer.append("column-cell-supersparse " + benchmark.columnTimesCell(superSparseCellsZero) + "ms\n")
    buffer.append("\n")

    benchmark.shutdown()
  }

  def runColumnColumn(buffer: StringBuilder) = {

    val benchmark = new MatrixBenchmark(sparkMaster, pathToMatrix, numColumns, degreeOfParallelism)

    val denseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 1.0, true)
    val sparseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 0.1, true)
    val superSparseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 0.01, true)

    buffer.append("column-column-dense " + benchmark.columnTimesCell(denseCellsZero) + "ms\n")
    buffer.append("column-column-sparse " + benchmark.columnTimesCell(sparseCellsZero) + "ms\n")
    buffer.append("column-column-supersparse " + benchmark.columnTimesCell(superSparseCellsZero) + "ms\n")
    buffer.append("\n")

    benchmark.shutdown()
  }

  def runCellColumn(buffer: StringBuilder) = {

    val benchmark = new MatrixBenchmark(sparkMaster, pathToMatrix, numColumns, degreeOfParallelism)

    val denseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 1.0, true)
    val sparseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 0.1, true)
    val superSparseCellsZero = MatrixBenchmark.generateVectorEntries(numColumns, 0.01, true)

    buffer.append("cell-column-dense " + benchmark.cellTimesColumn(denseCellsZero, true) + "ms\n")
    buffer.append("cell-column-sparse " + benchmark.cellTimesColumn(sparseCellsZero, false) + "ms\n")
    buffer.append("cell-column-supersparse " + benchmark.cellTimesColumn(superSparseCellsZero, false) + "ms\n")
    buffer.append("\n")

    benchmark.shutdown()
  }

}


case class Cell(row: Int, column: Int, value: Double)

object MatrixBenchmark {
  def asVector(vectorEntries: Seq[Cell], numColumns: Int, dense: Boolean) = {
    if (!dense) {
      val tmpVector = new RandomAccessSparseVector(numColumns)
      for (entry <- vectorEntries) {
        tmpVector.setQuick(entry.row, entry.value)
      }
      new SequentialAccessSparseVector(tmpVector)
    } else {
      val tmpVector = new DenseVector(numColumns)
      for (entry <- vectorEntries) {
        tmpVector.setQuick(entry.row, entry.value)
      }
      tmpVector
    }
  }

  def generateVectorEntries(numColumns: Int, density: Double, startAtZero: Boolean) = {
    if (startAtZero) {
      for (index <- 0 until numColumns if math.random <= density) yield { Cell(index, 1, 3.14) }
    } else {
      for (index <- 1 to numColumns if math.random <= density) yield { Cell(index, 1, 3.14) }
    }
  }
}

class MatrixBenchmark(sparkMaster: String, pathToMatrix: String, numColumns: Int, degreeOfParallelism: Int)
    extends Serializable {

  System.setProperty("spark.serializer", classOf[KryoSerializer].getName)
  System.setProperty("spark.kryo.registrator", classOf[MahoutKryoRegistrator].getName)

  @transient val ctx = new SparkContext(sparkMaster, "Gilbert")

  @transient val cellPartitionedMatrix = loadCellPartitionedMatrix()
  @transient val rowPartitionedMatrix = loadRowPartitionedMatrix()
  @transient val columnPartitionedMatrix = loadColumnPartitionedMatrix()

  def shutdown() = {
    ctx.stop()
    Thread.sleep(3000)
  }

  private def createAndCache[T <: Any](matrix: RDD[T]): RDD[T] = {
    matrix.cache()
    matrix.first()
    matrix
  }

  private def loadCellPartitionedMatrix() = {
    val matrix = ctx.textFile(pathToMatrix, degreeOfParallelism)
      .map({ line => {
      val fields = line.split(",")
      Cell(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }})

    createAndCache(matrix)
  }

  private def loadRowPartitionedMatrix() = {
    val matrix = ctx.textFile(pathToMatrix, degreeOfParallelism)
      .map({ line => {
        val fields = line.split(",")
        Cell((fields(0).toInt - 1), (fields(1).toInt - 1), fields(2).toDouble)
      }})
      .groupBy(_.row).flatMap({ case (index, cells) => {
        val vector = new RandomAccessSparseVector(numColumns)
        for (cell <- cells) {
          vector.setQuick(cell.column, cell.value)
        }
        Seq((index, new SequentialAccessSparseVector(vector)))
    }})

    createAndCache(matrix)
  }

  private def loadColumnPartitionedMatrix() = {

    val matrix = ctx.textFile(pathToMatrix, degreeOfParallelism)
      .map({ line => {
        val fields = line.split(",")
        Cell((fields(0).toInt - 1), (fields(1).toInt - 1), fields(2).toDouble)
      }})
      .groupBy(_.column).flatMap({ case (index, cells) => {
      val vector = new RandomAccessSparseVector(numColumns)
      for (cell <- cells) {
        vector.setQuick(cell.row, cell.value)
      }
      Seq((index, new SequentialAccessSparseVector(vector)))
    }})

    createAndCache(matrix)
  }

  def cellTimesCell(vectorEntries: Seq[Cell]) = {

    val start = System.currentTimeMillis

    val vector = scala.collection.mutable.Map[Int, Cell]()
    vectorEntries.foreach({ cell => vector.update(cell.row, cell) })

    ctx.broadcast(vector)

    cellPartitionedMatrix
        .filter({ cell => vector.contains(cell.column) })
        .map({ cell => {
          val correspondingCell = vector.get(cell.column).get
          (cell.row, cell.value * correspondingCell.value)
         }})
        .reduceByKey(_ + _).toArray()

    System.currentTimeMillis - start
  }
  
  def rowTimesColumn(vectorEntries: Seq[Cell], dense: Boolean) = {

    val vector = MatrixBenchmark.asVector(vectorEntries, numColumns, dense)

    val start = System.currentTimeMillis

    val wrappedVector = KryoSerializationWrapper(vector)
    ctx.broadcast(KryoSerializationWrapper(wrappedVector))

    rowPartitionedMatrix.map({ case (rowIndex, row) => {
      val columnVector = wrappedVector.value

      if (!columnVector.isDense) {
        (rowIndex, row.dot(wrappedVector.value))
      } else {
        var dot = 0.0
        for (elem <- row.nonZeroes()) {
          dot += elem.get() * columnVector.getQuick(elem.index())
        }
        (rowIndex, dot)
      }

    }}).toArray()

    System.currentTimeMillis - start
  }

  def rowTimesCell(vectorEntries: Seq[Cell]) = {

    val start = System.currentTimeMillis

    val vector = scala.collection.mutable.Map[Int, Cell]()
    vectorEntries.foreach({ cell => vector.update(cell.row, cell) })

    ctx.broadcast(vector)

    rowPartitionedMatrix.map({ case (index, row) => {
      var dot = 0.0
      for (elem <- row.nonZeroes()) {
        vector.get(elem.index()) match {
          case cell: Some[Cell] => { dot += elem.get() * cell.get.value }
          case _ =>
        }
      }
      Cell(index, 1, dot)
    }}).toArray()

    System.currentTimeMillis - start
  }

  def columnTimesCell(vectorEntries: Seq[Cell]) = {

    val start = System.currentTimeMillis

    val result = ctx.parallelize(vectorEntries, degreeOfParallelism).groupBy(_.row)
    .join(columnPartitionedMatrix).map({ case (index, (cells, column)) => {
        column.times(cells.head.value)
    }})
    .reduce({ case (aggregate, vector) => aggregate.assign(vector, Functions.PLUS)  })

    result.getNumNonZeroElements

    System.currentTimeMillis - start
  }

  def columnTimesColumn(vectorEntries: Seq[Cell], dense: Boolean) = {

    val mahoutVector = MatrixBenchmark.asVector(vectorEntries, numColumns, dense)

    val start = System.currentTimeMillis

    val cells = new util.ArrayList[(Int,Double)](mahoutVector.getNumNonZeroElements)
    
    for (elem <- mahoutVector.nonZeroes()) {
      cells.add((elem.index(), elem.get()))
    }

    val result = ctx.parallelize(cells, degreeOfParallelism).groupBy(_._1)
      .join(columnPartitionedMatrix).map({ case (_, (cells, column)) => {
        column.times(cells.head._2)
      }})
      .reduce({ case (aggregate, vector) => aggregate.assign(vector, Functions.PLUS) })

    result.getNumNonZeroElements

    System.currentTimeMillis - start
  }

  def cellTimesColumn(vectorEntries: Seq[Cell], dense: Boolean) = {

    val vector = MatrixBenchmark.asVector(vectorEntries, numColumns, dense)

    val start = System.currentTimeMillis

    val wrappedVector = KryoSerializationWrapper(vector)
    ctx.broadcast(KryoSerializationWrapper(wrappedVector))

    cellPartitionedMatrix.groupBy(_.row).map({ case (index, cells) => {
      val entries = scala.collection.mutable.Map[Int,Double]()
      for (cell <- cells) {
        entries.update(cell.column, cell.value)
      }
      var dot = 0.0
      val columnVector = wrappedVector.value
      for (elem <- columnVector.nonZeroes()) {
        entries.get(elem.index()) match {
          case entry: Some[Double] => { dot += elem.get() * entry.get }
          case _ =>
        }
      }
      Cell(index, 1, dot)
    }}).toArray()

    System.currentTimeMillis - start
  }

}


