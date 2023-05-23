package pack

object Scenerio15 {
  def main(args: Array[String]): Unit = {
    val l1 = List(2, 3, 4, 5)
    val l2 = List(6, 7, 8, 9)
    //append
    val appendlst = l1 ::: l2
    println(appendlst)

    //extending list
    val extendlst = l1 ++ l2
    println(extendlst)
  }
}