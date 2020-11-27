package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.Stats.ValueFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.column.statistics._
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageType
import java.lang.{Integer => JInt}


import scala.jdk.CollectionConverters._
import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.reflect.classTag

object ColumnStats {

  def apply[T <: Comparable[T], V](statistics: Statistics[T])
                                  (implicit valueFactory: ValueFactory[T, V]): ColumnStats[T, V] =
    ColumnStats(
      min = statistics.genericGetMin(),
      max = statistics.genericGetMax,
      valueFactory = valueFactory
    )


}

case class IntColumnStats(statistics: IntStatistics)
  extends ColumnStats[JInt, Int](statistics.genericGetMin(), statistics.genericGetMax, Stats.intFactory)

case class ColumnStats[T <: Comparable[T], V](min: T, max: T, valueFactory: ValueFactory[T, V]) {

  def updated(statistics: Statistics[T]): ColumnStats[T, V] = {
    val hasNewMin = statistics.compareMinToValue(min) < 0
    val hasNewMax = statistics.compareMaxToValue(max) > 0
    if (!hasNewMin && !hasNewMax) this
    else {
      val newMin = if (hasNewMin) statistics.genericGetMin else min
      val newMax = if (hasNewMax) statistics.genericGetMax else max
      this.copy(min = newMin, max = newMax)
    }
  }

  private def convert[V](t: T, vcc: ValueCodecConfiguration)(implicit codec: ValueCodec[V]) =
    codec.decode(valueFactory(t), vcc)

  def getMin[V](vcc: ValueCodecConfiguration)(implicit codec: ValueCodec[V]): Option[V] =
    Option(convert(min, vcc))

  def getMax[V](vcc: ValueCodecConfiguration)(implicit codec: ValueCodec[V]): Option[V] =
    Option(convert(max, vcc))

}

class Stats(val recordCount: Long, columnStats: Map[String, ColumnStats[_, _]], vcc: ValueCodecConfiguration) {

  def min[V](dotPath: String)(implicit codec: ValueCodec[V]): Option[V] =
    columnStats.get(dotPath).flatMap(_.getMin(vcc))

  def max[V](dotPath: String)(implicit codec: ValueCodec[V]): Option[V] =
    columnStats.get(dotPath).flatMap(_.getMax(vcc))

}

object Stats {

  type ValueFactory[T <: Comparable[T], V] = T => PrimitiveValue[V]

  implicit val intFactory: ValueFactory[java.lang.Integer, Int] = integer => IntValue(integer)
  implicit val longFactory: ValueFactory[java.lang.Long, Long] = long => LongValue(long)
  //  implicit val intFactory: ValueFactory[java.lang.Integer, Int] = integer => IntValue(integer)
  //  implicit val intFactory: ValueFactory[java.lang.Integer, Int] = integer => IntValue(integer)
  //  implicit val intFactory: ValueFactory[java.lang.Integer, Int] = integer => IntValue(integer)

  def apply(
             path: Path,
             conf: Configuration,
             filter: Filter,
             options: ParquetReader.Options,
             projectionSchemaOpt: Option[MessageType]
           ): Stats = {
    val inputFile = HadoopInputFile.fromPath(path, conf)
    val filterCompat = filter.toFilterCompat(options.toValueCodecConfiguration)
    val hOpts = ParquetReadOptions.builder()
      .withRecordFilter(filterCompat)
      .build

    val reader = ParquetFileReader.open(inputFile, hOpts)
    projectionSchemaOpt.foreach(reader.setRequestedSchema)

    try {
      val recordCount = reader.getFilteredRecordCount

      val columnStats = reader.getRowGroups.asScala.flatMap(_.getColumns.asScala).foldLeft(Map.empty[String, ColumnStats[_, _]]) {
        // TODO test with optionals
        case (statsMap, column) if !column.getStatistics.isEmpty =>
          val dotPath = column.getPath.toDotString
          val currentColumnStatsOpt = statsMap.get(dotPath)
          (column.getStatistics, currentColumnStatsOpt) match {
            case (s: IntStatistics, opt: Option[IntColumnStats]) =>

              statsMap.updated(dotPath, opt.fold[ColumnStats[JInt, Int]](IntColumnStats(s))(_.updated(s)))

            case s: LongStatistics =>
              update(statsMap, s, column)
//            case s: BooleanStatistics =>
//              ColumnStats.update(statsMap, s, column)
//            case s: BinaryStatistics =>
//              ColumnStats.update(statsMap, s, column)
//            case s: DoubleStatistics =>
//              ColumnStats.update(statsMap, s, column, DoubleValue.apply)
//            case s: FloatStatistics =>
//              ColumnStats.update(statsMap, s, column, FloatValue.apply)
          }
        case (statsMap, _) =>
          statsMap
      }
      new Stats(recordCount, columnStats, options.toValueCodecConfiguration)
    } finally {
      reader.close()
    }
  }

  def update[T <: Comparable[T], V](statistics: Statistics[T])
                                   (implicit valueFactory: ValueFactory[T, V]): Map[String, ColumnStats[_, _]] = {

        map.updated(dotPath, columnStats.updated(statistics))
    }
  }

}
