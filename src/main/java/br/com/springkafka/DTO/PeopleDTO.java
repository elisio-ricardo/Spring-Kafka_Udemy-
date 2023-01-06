package br.com.springkafka.DTO;

import lombok.Getter;

import java.util.List;
@Getter
public class PeopleDTO {

    private String name;
    private String cpf;
    private List<String> books;
}
