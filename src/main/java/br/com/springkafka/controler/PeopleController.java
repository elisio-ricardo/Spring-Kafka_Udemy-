package br.com.springkafka.controler;



import br.com.springkafka.DTO.PeopleDTO;
import br.com.springkafka.People;
import br.com.springkafka.producer.PeopleProducer;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.UUID;
import java.util.stream.Collectors;

@Controller
@RequestMapping("/people")
@AllArgsConstructor
public class PeopleController {

    private final PeopleProducer peopleProducer;

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> sendMessage(@RequestBody PeopleDTO peopleDTO) {
        var id = UUID.randomUUID().toString();

        var message = People.newBuilder() // classe people do avro
                .setId(id)
                .setCpf(peopleDTO.getCpf())
                .setName(peopleDTO.getName())
                .setBooks(peopleDTO.getBooks().stream().map(p -> (CharSequence) p).collect(Collectors.toList()))
                .build();
        peopleProducer.sendMessage(message);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

}
