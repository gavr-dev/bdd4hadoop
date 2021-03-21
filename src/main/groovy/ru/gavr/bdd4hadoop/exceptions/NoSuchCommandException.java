package ru.gavr.bdd4hadoop.exceptions;

public class NoSuchCommandException extends RuntimeException {
    public NoSuchCommandException(String message) {
        super(message);
    }
}
