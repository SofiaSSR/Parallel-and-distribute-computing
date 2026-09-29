package co.edu.unal.paralela;

<<<<<<< HEAD
=======
import java.util.concurrent.ForkJoinPool;
>>>>>>> 0e8c608b9f4ab7b595c2605380bd64569281626c
import java.util.concurrent.RecursiveAction;

/**
 * Clase que contiene los métodos para implementar la suma de los recíprocos de un arreglo usando paralelismo.
 */
public final class ReciprocalArraySum {

    private static final ForkJoinPool POOL = ForkJoinPool.commonPool();

    /**
     * Constructor.
     */
    private ReciprocalArraySum() {
    }

    /**
     * Calcula secuencialmente la suma de valores recíprocos para un arreglo.
     *
     * @param input Arreglo de entrada
     * @return La suma de los recíprocos del arreglo de entrada
     */
    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        // Calcula la suma de los recíprocos de los elementos del arreglo
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }

        return sum;
    }

    /**
     * calcula el tamaño de cada trozo o sección, de acuerdo con el número de secciones para crear
     * a través de un número dado de elementos.
     *
     * @param nChunks El número de secciones (chunks) para crear
     * @param nElements El número de elementos para dividir
     * @return El tamaño por defecto de la sección (chunk)
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Función techo entera
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Calcula el índice del elemento inclusivo donde la sección/trozo (chunk) inicia,
     * dado que hay cierto número de secciones/trozos (chunks).
     *
     * @param chunk la sección/trozo (chunk) para cacular la posición de inicio
     * @param nChunks Cantidad de secciones/trozos (chunks) creados
     * @param nElements La cantidad de elementos de la sección/trozo que deben atravesarse
     * @return El índice inclusivo donde esta sección/trozo (chunk) inicia en el conjunto de
     *         nElements
     */
    private static int getChunkStartInclusive(final int chunk,
            final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    /**
     * Calcula el índice del elemento exclusivo que es proporcionado al final de la sección/trozo (chunk),
     * dado que hay cierto número de secciones/trozos (chunks).
     *
     * @param chunk La sección para calcular donde termina
     * @param nChunks Cantidad de secciones/trozos (chunks) creados
     * @param nElements La cantidad de elementos de la sección/trozo que deben atravesarse
     * @return El índice de terminación exclusivo para esta sección/trozo (chunk)
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }

    /**
     * Tarea hoja: suma los recíprocos de un único rango [startIndexInclusive, endIndexExclusive)
     * del arreglo. No se subdivide a sí misma; la división en tareas ocurre una sola vez,
     * antes de crear las instancias.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {
<<<<<<< HEAD
        private final int startIndexInclusive;
        private final int endIndexExclusive;
        private final double[] input;
        private double value;

=======
        /**         * Iniciar el índice para el recorrido transversal hecho por esta tarea.         */
        private final int startIndexInclusive;/**   * Concluir el índice para el recorrido transversal hecho por esta tarea.         */
        private final int endIndexExclusive;        /**         * Arreglo de entrada para la suma de recíprocos.         */
        private final double[] input;        /**         * Valor intermedio producido por esta tarea.         */
        protected double value = 0;
        private final int numTasks;
        /**         * Constructor.
         * * @param setStartIndexInclusive establece el índice inicial para comenzar
         *        el recorrido trasversal.
         *        * @param setEndIndexExclusive establece el índice final para el recorrido trasversal.
         * @param setInput Valores de entrada
         */
>>>>>>> 0e8c608b9f4ab7b595c2605380bd64569281626c
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                final int setEndIndexExclusive, final double[] setInput, final int tSize) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
            this.numTasks=tSize;
        }

        public double getValue() {
            return this.value;
        }
         @Override
        protected void compute() {
<<<<<<< HEAD
            // Copias locales: evita releer campos del objeto en cada iteración
            // y evita false sharing (solo se escribe "value" una vez, al final).
            final double[] in = input;
            final int end = endIndexExclusive;
            double parcial = 0;
            for (int i = startIndexInclusive; i < end; i++) {
                parcial += 1 / in[i];
            }
            value = parcial;
        }
=======
            if (endIndexExclusive - startIndexInclusive == numTasks) {
                double parcial = 0;
                for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
                    parcial += 1 / input[i];
                }
                value = parcial;
                return;
            }
            final ReciprocalArraySumTask[] tasks = new ReciprocalArraySumTask[numTasks];
            final int chunkSize = getChunkSize(numTasks, input.length);
            for (int i = 1; i < numTasks; i++) {
                tasks[i] = new ReciprocalArraySumTask(
                        getChunkStartInclusive(i, numTasks, input.length),
                        getChunkEndExclusive(i, numTasks, input.length), input, chunkSize);
                tasks[i].fork();
            }
            tasks[0] = new ReciprocalArraySumTask(
                    getChunkStartInclusive(0, numTasks, input.length),
                    getChunkEndExclusive(0, numTasks, input.length), input, chunkSize);
            tasks[0].compute();
            for (int i = 1; i < numTasks; i++) {
                tasks[i].join();
            }
            for (ReciprocalArraySumTask task : tasks) {
                value += task.getValue();
            }
        } 
       //Versión recursiva
       /* @Override 
       protected void compute() {

            if (numTasks == 1) { // Umbral para dividir
                   double parcial = 0;
                for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
                    parcial += 1 / input[i];
                }
                value = parcial;
            } else {
                 ReciprocalArraySumTask rightTask = new ReciprocalArraySumTask(0, getChunkEndExclusive(numTasks-2, numTasks, endIndexExclusive), input, numTasks-1);
                
                rightTask.fork(); // Ejecuta la tarea derecha en el hilo actual

                double parcial = 0;
                for (int i = getChunkStartInclusive(numTasks-1, numTasks, endIndexExclusive); i < endIndexExclusive; i++) {
                    parcial += 1 / input[i];
                }
                rightTask.join(); // Espera a que la tarea derecha termine

                value = parcial + rightTask.getValue(); // Suma los resultados                
            }
        }*/
       /* 
       //Más eficiente que la versión recursiva, pero no es la más eficiente
       @Override 
       protected void compute() {

            if (endIndexExclusive - startIndexInclusive <= getChunkSize(numTasks, input.length)) { // Umbral para dividir
                   double parcial = 0;
                for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
                    parcial += 1 / input[i];
                }
                value = parcial;
            } else {
                 int mid = (startIndexInclusive + endIndexExclusive) / 2;
                 ReciprocalArraySumTask leftTask = new ReciprocalArraySumTask(startIndexInclusive, mid, input, numTasks);
                 ReciprocalArraySumTask rightTask = new ReciprocalArraySumTask(mid, endIndexExclusive, input, numTasks);
                
                leftTask.fork(); // Ejecuta la tarea izquierda en un hilo separado
                rightTask.compute(); // Ejecuta la tarea derecha en el hilo actual
                leftTask.join(); // Espera a que la tarea izquierda termine

                value = leftTask.getValue() + rightTask.getValue(); // Suma los resultados                
            }
        } */
       /* @Override
       protected void compute() {
            final int chunkSize = getChunkSize(numTasks, input.length);
            final int rangeSize = endIndexExclusive - startIndexInclusive;

            if (rangeSize <= chunkSize) {
                double parcial = 0;
                for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
                    parcial += 1 / input[i];
                }
                value = parcial;
            } else {
                final int chunkEndExclusive = startIndexInclusive + chunkSize;
                final ReciprocalArraySumTask chunkTask = new ReciprocalArraySumTask(
                        startIndexInclusive, chunkEndExclusive, input, numTasks);
                final ReciprocalArraySumTask remainingTask = new ReciprocalArraySumTask(
                        chunkEndExclusive, endIndexExclusive, input, numTasks);

                chunkTask.fork();
                remainingTask.compute();
                chunkTask.join();

                value = chunkTask.getValue() + remainingTask.getValue();
            }
        } */
     
        

       
>>>>>>> 0e8c608b9f4ab7b595c2605380bd64569281626c
    }

    /**
     * Calcula la suma de recíprocos utilizando exactamente dos tareas en paralelo.
     *
     * @param input Arreglo de entrada
     * @return La suma de los recíprocos del arreglo de entrada
     */
    protected static double parArraySum(final double[] input) {
        assert input.length % 2 == 0;

<<<<<<< HEAD
        final int mid = input.length / 2;
        final ReciprocalArraySumTask left = new ReciprocalArraySumTask(0, mid, input);
        final ReciprocalArraySumTask right = new ReciprocalArraySumTask(mid, input.length, input);
=======
        final ReciprocalArraySumTask task = new ReciprocalArraySumTask(0, input.length, input, 2);
>>>>>>> 0e8c608b9f4ab7b595c2605380bd64569281626c

<<<<<<< HEAD
        // fork() lanza "left" de forma asíncrona (usa el commonPool automáticamente
        // si no estamos ya dentro de un ForkJoinPool). "right" se ejecuta en este mismo hilo.
        left.fork();
        right.compute();
        left.join();

        return left.getValue() + right.getValue();
=======
        //Acá qué se supone que hago?
        task.fork(); // Inicia la tarea en un hilo separado
        task.join(); // Espera a que la tarea termine
        
        /*
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }*/
        return task.getValue();
>>>>>>> 0e8c608b9f4ab7b595c2605380bd64569281626c
    }

    /**
     * Calcula la suma de recíprocos utilizando un número arbitrario de tareas en paralelo.
     * Se calculan de antemano los numTasks rangos exactos y se lanza una tarea por cada uno.
     *
     * @param input Arreglo de entrada
     * @param numTasks El número de tareas para crear
     * @return La suma de los recíprocos del arreglo de entrada
     */
    protected static double parManyTaskArraySum(final double[] input,
            final int numTasks) {
<<<<<<< HEAD
        final ReciprocalArraySumTask[] tasks = new ReciprocalArraySumTask[numTasks];
=======
        final ReciprocalArraySumTask task = new ReciprocalArraySumTask(0, input.length, input, numTasks);
>>>>>>> 0e8c608b9f4ab7b595c2605380bd64569281626c

<<<<<<< HEAD
        for (int i = 0; i < numTasks; i++) {
            tasks[i] = new ReciprocalArraySumTask(
                    getChunkStartInclusive(i, numTasks, input.length),
                    getChunkEndExclusive(i, numTasks, input.length),
                    input);
        }

        // Lanza todas menos la primera; la primera se ejecuta en este hilo
        // para no desperdiciarlo esperando sin hacer nada.
        for (int i = 1; i < numTasks; i++) {
            tasks[i].fork();
        }
        tasks[0].compute();

        double sum = tasks[0].getValue();
        for (int i = 1; i < numTasks; i++) {
            tasks[i].join();
            sum += tasks[i].getValue();
        }

        return sum;
=======
        //Acá qué se supone que hago?
       task.fork(); // Inicia la tarea en un hilo separado
       task.join(); // Espera a que la tarea termines

        return task.getValue();
>>>>>>> 0e8c608b9f4ab7b595c2605380bd64569281626c
    }
}