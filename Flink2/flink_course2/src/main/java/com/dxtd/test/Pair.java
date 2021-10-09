package com.dxtd.test;

public class Pair<T,K> {
    private T name;
    private K age;

    public T getName() {
        return name;
    }

    public void setName(T name) {
        this.name = name;
    }

    public K getAge() {
        return age;
    }

    public void setAge(K age) {
        this.age = age;
    }

    public Pair(T name, K age) {
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return "Pair{" +
                "name=" + name +
                ", age=" + age +
                '}';
    }

    public static void main(String[] args) {
        Pair<String, Integer> pair  = new Pair<> ("wangwu",21);
        System.out.println(pair);



    }

}
