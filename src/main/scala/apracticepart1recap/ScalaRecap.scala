package apracticepart1recap

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {

  // values and variables
  val aBoolean: Boolean = false

  // expression
  val anIfExpression = if (2 > 3) "bigger" else "smaller"

  // instructions vs expressions
  val theUnit = println("Hello") // Unit => No "meaningful value"

  // functions
  def myFunction(x: Int) = 42

  // OOP
  class Animal
  class Dog extends Animal

  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = "Crunch!"
  }

  // singleton pattern
  object MySigleton {

  }

  object Carnivore


  // generics
  trait MyList[T]

  // method notation

  val x = 1 + 2
  val y = 1.+(2)

  // Functional Programming
  val incrementor: Function1[Int, Int] = new Function1[Int, Int] {
    override def apply(v1: Int): Int = v1 + 1
  }

  println(incrementor(42))

  val processedList = List(1,2,3).map(incrementor)

  // pattern matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  // try - catch

  try {
    throw new NullPointerException
  } catch {
    case e: NullPointerException => "something"
    case _ => ""
  }

  // Future
  val aFuture = Future {
    // some expensive computation run on another thread
    42
  }

  aFuture.onComplete {
    case Success(value) => println("got value")
    case Failure(exception) => println("Failed" + exception.getMessage)
  }

  // Partial function

  val aPF = (x: Int) => x match {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

  // Implicits
  // auto inject by compiler
  def methodWithImplicitArgs(implicit x: Int): Int = {
    x + 43
  }
  implicit val impInt = 67
  val impCall = methodWithImplicitArgs

  // implicit conversion - implicit defs

  case class Person(name: String) {
    def greet = println("Hi my name is " + name)
  }
  implicit def fromStringToPerson(name: String) = Person(name)

  "Bob".greet

  implicit class GoodDog(name: String){
    def bark = println("Bark!")
  }

  "Lassie".bark
































}
