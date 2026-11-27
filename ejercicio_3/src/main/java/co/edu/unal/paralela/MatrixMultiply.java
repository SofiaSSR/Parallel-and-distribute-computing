package co.edu.unal.paralela;

import static edu.rice.pcdp.PCDP.forall2d;
import static edu.rice.pcdp.PCDP.forallChunked;
import static edu.rice.pcdp.PCDP.forseq2d;

/**
 * Clase envolvente pata implementar de forma eficiente la multiplicación dde matrices en paralelo.
 */
public final class MatrixMultiply {
    /**
     * Constructor por omisión.
     */
    private MatrixMultiply() {
    }

    /**
     * Realiza una multiplicación de matrices bidimensionales (A x B = C) de forma secuencial.
     *
     * @param A Una matriz de entrada con dimensiones NxN
     * @param B Una matriz de entrada con dimensiones NxN
     * @param C Matriz de salida
     * @param N Tamaño de las matrices de entrada
     */
    public static void seqMatrixMultiply(final double[][] A, final double[][] B,
            final double[][] C, final int N) {
        forseq2d(0, N - 1, 0, N - 1, (i, j) -> {
            C[i][j] = 0.0;
            for (int k = 0; k < N; k++) {
                C[i][j] += A[i][k] * B[k][j];
            }
        });
    }

    /**
     * Realiza una multiplicación de matrices bidimensionales (A x B = C) de forma paralela.
     *
     * Se paraleliza el ciclo externo (filas i) con forallChunked: cada worker recibe
     * un bloque contiguo de filas de C, así se crean pocas tareas (una por chunk)
     * en vez de N*N. Las filas de C son independientes, por lo que no hay carreras.
     *
     * @param A Una matriz de entrada con dimensiones NxN
     * @param B Una matriz de entrada con dimensiones NxN
     * @param C Matriz de salida
     * @param N amaño de las matrices de entrada
     */
    public static void parMatrixMultiply(final double[][] A, final double[][] B,
            final double[][] C, final int N) {
        forallChunked(0, N - 1, i -> {
            final double[] Ci = C[i];
            java.util.Arrays.fill(Ci, 0.0);
            // Orden i-k-j: B se recorre por filas (acceso contiguo en memoria, amigable
            // con la caché). Cada C[i][j] sigue sumando en el orden k = 0..N-1, así que
            // el resultado es idéntico al de la versión secuencial.
            for (int k = 0; k < N; k++) {
                final double aik = A[i][k];
                final double[] Bk = B[k];
                for (int j = 0; j < N; j++) {
                    Ci[j] += aik * Bk[j];
                }
            }
        });
    }

    /**
     * Versión con forall2d (una tarea por celda (i, j)), solo para comparar con
     * parMatrixMultiply. Es correcta pero crea N*N tareas, y ese overhead la hace
     * más lenta que la versión con forallChunked.
     *
     * @param A Una matriz de entrada con dimensiones NxN
     * @param B Una matriz de entrada con dimensiones NxN
     * @param C Matriz de salida
     * @param N Tamaño de las matrices de entrada
     */
    public static void parMatrixMultiplyForall(final double[][] A, final double[][] B,
            final double[][] C, final int N) {
        forall2d(0, N - 1, 0, N - 1, (i, j) -> {
            double sum = 0.0;
            for (int k = 0; k < N; k++) {
                sum += A[i][k] * B[k][j];
            }
            C[i][j] = sum;
        });
    }
}
