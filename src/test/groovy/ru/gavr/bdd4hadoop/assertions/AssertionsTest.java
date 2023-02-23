package ru.gavr.bdd4hadoop.assertions;

import groovy.json.JsonOutput;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class AssertionsTest {
    String received = "Success, location = /tmp";

    @Test
    public void isContainsTest() {
        ArrayList results = (ArrayList) Assertions.isContains("received", "location =", received);
        assertTrue((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("received contains \"location =\"");

        results = (ArrayList) Assertions.isContains("received", "Failure", received);
        assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("received contains \"Failure\"");
        assertThat((String) results.get(2)).contains(String.format("received contains \"%s\"", received));
    }

    @Test
    public void isNotContainsTest() {
        ArrayList results = (ArrayList) Assertions.isNotContains("received", "location =", received);
        assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("received not contains \"location =\"");
        assertThat((String) results.get(2)).contains("received contains \"location =\"");

        results = (ArrayList) Assertions.isNotContains("received", "Failure", received);
        assertTrue((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("received not contains \"Failure\"");
    }

    @Test
    public void isEmptyTest() {
        ArrayList results = (ArrayList) Assertions.isEmpty(JsonOutput.toJson(received));
        assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("result is empty");
        assertThat((String) results.get(2)).contains(String.format("result \"%s\" is not empty", received));

        results = (ArrayList) Assertions.isEmpty(JsonOutput.toJson(""));
        assertTrue((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("result is empty");

        results = (ArrayList) Assertions.isEmpty(JsonOutput.toJson(new ArrayList<>()));
        assertTrue((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("result is empty");
    }

    @Test
    public void isNothingTest() {
        ArrayList results = (ArrayList) Assertions.isNothing(JsonOutput.toJson("NO DATA"));
        assertTrue((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("result is nothing");

        results = (ArrayList) Assertions.isNothing(JsonOutput.toJson(received));
        assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("result is nothing");
        assertThat((String) results.get(2)).contains(String.format("result \"%s\" is not nothing", received));
    }

    @Test
    public void containsLine() {
        List<String> lines = Arrays.asList("First line", "SecondLine");
        ArrayList results = (ArrayList) Assertions.containsLine(lines, 0, "First line");
        assertTrue((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("First line");
        assertThat((String) results.get(2)).contains("First line");

        results = (ArrayList) Assertions.containsLine(lines, 4, "First line");
        assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("First line");
        assertThat((String) results.get(2)).contains("null");

        results = (ArrayList) Assertions.containsLine(lines, 0, "Random line");
        assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("Random line");
        assertThat((String) results.get(2)).contains("First line");
    }

    @Test
    public void lineStartWith() {
        List<String> lines = Arrays.asList("First line", "SecondLine");
        ArrayList results = (ArrayList) Assertions.lineStartWith(lines, 0, "First");
        assertTrue((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("First");
        assertThat((String) results.get(2)).contains("First line");

        results = (ArrayList) Assertions.lineStartWith(lines, 4, "First");
        assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("First");
        assertThat((String) results.get(2)).contains("null");

        results = (ArrayList) Assertions.lineStartWith(lines, 0, "Random");
        assertFalse((Boolean) results.get(0));
        assertThat((String) results.get(1)).contains("Random");
        assertThat((String) results.get(2)).contains("First line");
    }
}