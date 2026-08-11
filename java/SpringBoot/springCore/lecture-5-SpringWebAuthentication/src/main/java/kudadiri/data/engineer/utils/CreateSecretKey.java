package kudadiri.data.engineer.utils;

import io.jsonwebtoken.Jwts;

import java.util.Base64;

public class CreateSecretKey {
    public static void main(String[] args) {
        String secret = Base64.getEncoder().encodeToString(
                Jwts.SIG.HS512.key().build().getEncoded()
        );

        System.out.println(secret);
    }
}