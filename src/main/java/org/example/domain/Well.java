package org.example.domain;

import java.util.List;

public record Well(Long id, String name, List<Equipment> equipments) {
}
