package data.engineer.service;

import data.engineer.entity.Book;
import data.engineer.repository.BookRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class BookService {
    private final BookRepository repository;

    public List<Book> getAllBooks() {
        return repository.findAll();
    }

    public Book getBookById(Long id) {
        return repository.findById(id).orElseThrow(
                () -> new RuntimeException("Book not found with id: " + id)
        );
    }

    public List<Book> getBookByAuthorName(String author) {
        return repository.findByAuthorContainingIgnoreCase(author);
    }

    public Book addBook(Book book) {
        return repository.save(book);
    }

    public Book updateBook(Long id, Book book) {
        Book data = repository.findById(id).orElseThrow(
                () -> new RuntimeException("Book not found with id: " + id)
        );

        data.setTitle(book.getTitle());
        data.setAuthor(book.getAuthor());
        data.setPublisher(book.getPublisher());
        data.setStock(book.getStock());
        data.setPublicationYear(book.getPublicationYear());

        return repository.save(data);
    }

    public void deleteBook(Long id) {
        if (!repository.existsById(id)) {
            throw new RuntimeException("Book not found with id: " + id);
        }

        repository.deleteById(id);
    }
}