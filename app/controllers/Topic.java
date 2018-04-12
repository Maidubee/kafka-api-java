package controllers;

public class Topic {

    private String name;
    private int randomNumber;

    public Topic(String name, int randomNumber) {
        this.setName(name);
        this.setRandomNumber(randomNumber);
        System.out.println("Passed name is: " + name);
        System.out.println("Passed number is: " + randomNumber);
    }

    private void setName(String name){
        this.name = name;
    }

    private void setRandomNumber(int randomNumber){
        this.randomNumber = randomNumber;
    }

    public String getName() {
        return this.name;
    }

    public int getRandomNumber() {
        return this.randomNumber;
    }
}
