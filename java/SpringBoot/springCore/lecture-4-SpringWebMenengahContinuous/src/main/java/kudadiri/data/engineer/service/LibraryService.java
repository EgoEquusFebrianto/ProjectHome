package kudadiri.data.engineer.service;

import kudadiri.data.engineer.dto.request.CreateBookRequest;
import kudadiri.data.engineer.dto.request.UpdateBookRequest;
import kudadiri.data.engineer.dto.response.BookResponse;
import kudadiri.data.engineer.entity.Book;
import kudadiri.data.engineer.repository.LibraryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class LibraryService {

    private final LibraryRepository repository;

    public BookResponse mapToResponse(Book book) {
        return BookResponse.builder()
                .id(book.getId())
                .title(book.getTitle())
                .author(book.getAuthor())
                .publisher(book.getPublisher())
                .publicationYear(book.getPublicationYear())
                .stock(book.getStock())
                .build();
    }

    public List<BookResponse> getAllBooks() {
        return repository.findAll()
                .stream()
                .map(this::mapToResponse)
                .toList();
    }

    public BookResponse getBookById(Long id) {
        Book book = repository.findById(id).orElseThrow(
                () -> new RuntimeException("Book not found with id: " + id)
        );

        return mapToResponse(book);
    }

    public List<BookResponse> getBookByAuthorName(String author) {
        return repository.findBookByAuthor(author)
                .stream()
                .map(this::mapToResponse)
                .toList();
    }

    public BookResponse createBook(CreateBookRequest request) {
        Book book = new Book();

        book.setTitle(request.getTitle());
        book.setAuthor(request.getAuthor());
        book.setPublisher(request.getPublisher());
        book.setPublicationYear(request.getPublicationYear());
        book.setStock(request.getStock());

        Book savedBook = repository.save(book);

        return mapToResponse(savedBook);
    }

    public BookResponse updateBook(Long id, UpdateBookRequest request) {
        Book book = repository.findById(id).orElseThrow(
                () -> new RuntimeException("Book not found with id: " + id)
        );

        book.setTitle(request.getTitle());
        book.setAuthor(request.getAuthor());
        book.setPublisher(request.getPublisher());
        book.setPublicationYear(request.getPublicationYear());
        book.setStock(request.getStock());

        Book updatedBook = repository.save(book);

        return mapToResponse(updatedBook);
    }

    public void deleteBook(Long id) {
        if (!repository.existsById(id)) {
            throw new RuntimeException("Book not found with id:" + id);
        }

        repository.deleteById(id);
    }
}
