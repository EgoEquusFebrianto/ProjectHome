package kudadiri.data.engineer.dto.request;

import lombok.Data;

@Data
public class CreateBookRequest {
    private String title;
    private String author;
    private String publisher;
    private Integer publicationYear;
    private Integer stock;
}