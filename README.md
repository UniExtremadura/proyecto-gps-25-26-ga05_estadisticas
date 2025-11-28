# Undersounds: Microservicio de Estadísticas

## Requisitos
* Docker
* Docker Compose

## Ejecución del servicio
1. Clona el proyecto.
2. Ejecuta el comando `docker compose up` desde la raíz del proyecto.
3. Espera a que el servicio esté listo (puede tardar unos 20-30 segundos).
4. ¡Listo! El microservicio estará disponible en:
   * API: `http://localhost:8083`
   * BD: `localhost:9042`

## Detención del servicio
Ejecuta `docker compose down` si quieres mantener los cambios en la BD, y `docker compose down -v` si quieres borrar el volumen de datos.
