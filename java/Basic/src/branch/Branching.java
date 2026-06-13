package branch;

public class Branching {
    public static void main(String[] args) {
        var absen = 2;
        var nilai = 70;

        if (absen < 5 && nilai >= 85) {
            System.out.println("Lulus Dengan Memuaskan");
        } else if(absen < 5 && nilai >= 70) {
            System.out.println("Lulus");
        } else {
            System.out.println("Tidak Lulus");
        }
    }
}
