package branch;

public class SwitchStatement {
    public static void main(String[] args) {
        var nilai = "B";

        // Versi Lama
        switch (nilai) {
            case "A":
                System.out.println("Lulus dengan Sempurna");
                break;
            case "B":
                System.out.println("Lulus");
                break;
            case "C":
                System.out.println("Cukup");
                break;
            case "D":
                System.out.println("Kurang");
                break;
            default:
                System.out.println("Tidak Lulus");
        }

        // Versi Lambda
        char nilai_siswa = 'E';
        switch (nilai_siswa) {
            case 'A' -> System.out.println("Lulus dengan Sempurna");
            case 'B' -> System.out.println("Lulus");
            case 'C' -> System.out.println("Cukup");
            case 'D' -> System.out.println("Kurang");
            default -> {
                String hasil = "Tidak Lulus";
                System.out.println(hasil);
            }
        }


        // Kata Kunci Yield
        // funsinya untuk mengembalikan nilai pada switch statement
        // ini sangat membantu bila butuh membuat data berdasarkan kondisi switch statement

        String ucapan;
        String ucapan2;

        // Tanpa kata Kunci yield kita menggunakan teknik switch lambda
        switch (nilai) {
            case "A" -> ucapan = "Average, good";
            case "B" -> ucapan = "What The HELL";
            case "C" -> ucapan = "I Think WE MUST BOUGHT DNA TEST";
            case "D" -> ucapan = "I HAVE NO SON/DAUGHTER";
            default -> {
                ucapan = "EXCUSE ME SIR/MA'AM, WHY YOU IN MY HOUSE?";
            }
        }

        System.out.println(ucapan);

        // Dengan Yield
        ucapan2 = switch (nilai) {
            case "A":
                yield "Average, good";
            case "B":
                yield "What The HELL";
            case "C":
                yield "I Think WE MUST BOUGHT DNA TEST";
            case "D":
                yield "I HAVE NO SON/DAUGHTER";
            default:
                yield "EXCUSE ME SIR/MA'AM, WHY YOU IN MY HOUSE?";
        };

        System.out.println(ucapan2);
    }
}
