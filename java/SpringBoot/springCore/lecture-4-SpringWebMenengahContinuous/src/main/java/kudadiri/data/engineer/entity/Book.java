package kudadiri.data.engineer.entity;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(
        name = "books",
        schema = "spring_learning"
)
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class Book {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String title;
    private String author;
    private String publisher;
    private Integer publicationYear;
    private Integer stock;
}