package io.goinhacker.boot2msaskeleton.userservice

import mu.KotlinLogging
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.data.annotation.CreatedDate
import org.springframework.data.annotation.Id
import org.springframework.data.keyvalue.repository.KeyValueRepository
import org.springframework.data.mongodb.config.EnableMongoAuditing
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.repository.ReactiveMongoRepository
import org.springframework.data.redis.core.RedisHash
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.server.HandlerFunction
import org.springframework.web.reactive.function.server.RequestPredicates.*
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.web.reactive.function.server.RouterFunctions.route
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import reactor.core.publisher.toMono
import java.net.URI
import java.time.LocalDateTime

@SpringBootApplication
@EnableMongoAuditing
class UserServiceApplication {

    @Bean
    fun routes(userHandler: UserHandler): RouterFunction<ServerResponse> {
        return route(GET("/users"), HandlerFunction<ServerResponse>(userHandler::all))
                .andRoute(POST("/users"), HandlerFunction<ServerResponse>(userHandler::create))
                .andRoute(GET("/users/{id}"), HandlerFunction<ServerResponse>(userHandler::get))
                .andRoute(PUT("/users/{id}"), HandlerFunction<ServerResponse>(userHandler::update))
                .andRoute(DELETE("/users/{id}"), HandlerFunction<ServerResponse>(userHandler::delete))
    }
}

fun main(args: Array<String>) {
    runApplication<UserServiceApplication>(*args)
}

@Component
class UserHandler(val users: UserRepository) {

    fun all(request: ServerRequest): Mono<ServerResponse> {
        return ok().body(this.users.findAll(), User::class.java)
    }

    fun create(request: ServerRequest): Mono<ServerResponse> {
        return request.bodyToMono(User::class.java)
                .flatMap { post -> this.users.save(post) }
                .flatMap { id -> created(URI.create("/posts/$id")).build() }
    }

    fun get(request: ServerRequest): Mono<ServerResponse> {
        return this.users.findById(request.pathVariable("id"))
                .flatMap { user -> ok().body(Mono.just(user), User::class.java) }
                .switchIfEmpty(notFound().build())
    }

    fun update(request: ServerRequest): Mono<ServerResponse> {
        return this.users.findById(request.pathVariable("id"))
                .zipWith(request.bodyToMono(User::class.java))
                .map { it.t1.copy(name = it.t2.name, age = it.t2.age) }
                .flatMap { this.users.save(it) }
                .flatMap { noContent().build() }
    }

    fun delete(request: ServerRequest): Mono<ServerResponse> {
        return this.users.deleteById(request.pathVariable("id"))
                .flatMap { noContent().build() }
    }
}

@Component
class AddTestDataInitializr(val users: UserRepository) : CommandLineRunner {
    private val log = KotlinLogging.logger {}

    override fun run(vararg strings: String) {
        log.info("start data initialization ...")
        this.users.deleteAll()
                .thenMany(Flux.just(
                        User(name = "Eun", age = 31), User(name = "Joe", age = 29), User(name = "Sua", age = 4))
                        .map { this.users.save(it) }
                )
                .log()
                .subscribe(null, null, { log.info("done initialization...") })
    }
}

interface CachedUserRepository : KeyValueRepository<User, String>

interface MongoUserRepository : ReactiveMongoRepository<User, String>

@Component
class UserRepository(val cacheRepo: CachedUserRepository, val mainRepo: MongoUserRepository) {

    fun findAll(): Flux<User> {
        return cacheRepo.findAll().toFlux().switchIfEmpty { mainRepo.findAll() }
    }

    fun findById(id: String): Mono<User> {
        val optional = cacheRepo.findById(id)
        if (optional.isPresent) {
            return optional.get().toMono()
        }

        return mainRepo.findById(id)
    }

    fun save(user: User): Mono<User> {
        return mainRepo.save(user).doOnSuccess { cacheRepo.save(user) }
    }

    fun deleteById(id: String): Mono<Unit> {
        return mainRepo.deleteById(id).doOnSuccess { cacheRepo.deleteById(id) }.map {  }
    }

    fun deleteAll(): Mono<Unit> {
        return mainRepo.deleteAll().doOnSuccess { cacheRepo.deleteAll() }.map {  }
    }
}

@Document
@RedisHash("users")
data class User(@Id val id: String? = null,
                val name: String? = null,
                val age: Int? = null,
                @CreatedDate val date: LocalDateTime = LocalDateTime.now())