package ru.gavr.bdd4hadoop.exceptions;

public class ConnectorException extends RuntimeException{
    public ConnectorException(String message, Throwable cause) {
        super(message, cause);
    }
}
