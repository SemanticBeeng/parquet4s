package com.github.mjakubowski84.parquet4s

import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag

// TODO use more performant Vector as a basis collection
trait CollectionTransformer[Element, Col[_]] {
  def from(col: Col[Element]): List[Element]
  def to(list: List[Element]): Col[Element]
}

trait CollectionTransformers {

  implicit def seqTransformer[E]: CollectionTransformer[E, Seq] = new CollectionTransformer[E, Seq] {
    override def from(col: Seq[E]): List[E] = col.toList
    override def to(list: List[E]): Seq[E] = list
  }

  implicit def listTransformer[E]: CollectionTransformer[E, List] = new CollectionTransformer[E, List] {
    override def from(col: List[E]): List[E] = col
    override def to(list: List[E]): List[E] = list
  }

  implicit def vectorTransformer[E]: CollectionTransformer[E, Vector] = new CollectionTransformer[E, Vector] {
    override def from(col: Vector[E]): List[E] = col.toList
    override def to(list: List[E]): Vector[E] = list.toVector
  }

  implicit def setTransformer[E]: CollectionTransformer[E, Set] = new CollectionTransformer[E, Set] {
    override def from(col: Set[E]): List[E] = col.toList
    override def to(list: List[E]): Set[E] = list.toSet
  }

  implicit def arrayTransformer[E : ClassTag]: CollectionTransformer[E, Array] = new CollectionTransformer[E, Array] {
    override def from(col: Array[E]): List[E] = col.toList
    override def to(list: List[E]): Array[E] = list.toArray
  }

}