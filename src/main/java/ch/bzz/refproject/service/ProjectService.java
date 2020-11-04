package ch.bzz.refproject.service;

import ch.bzz.refproject.data.Dao;
import ch.bzz.refproject.data.ProjectDao;
import ch.bzz.refproject.model.Category;
import ch.bzz.refproject.model.Project;
import ch.bzz.refproject.util.Result;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * services for projects
 * <p>
 * M151: Refproject
 *
 * @author Marcel Suter
 */
@Path("project")
public class ProjectService {

    /**
     * produces a list of all projects
     *
     * @param token encrypted authorization token
     * @return Response
     */
    @GET
    @Path("list")
    @Produces(MediaType.APPLICATION_JSON)

    public Response listProjects(
            @CookieParam("token") String token
    ) {

        int httpStatus = 200;
        Dao<Project, String> projectDao = new ProjectDao();
        List<Project> projectList = projectDao.getAll();
        if (projectList.isEmpty())
            httpStatus = 404;

        if (projectList.isEmpty()) {
            return Response
                    .status(404)
                    .entity("{\"error\":\"Keine Projekte gefunden\"}")
                    .build();
        } else {
            return Response
                    .status(httpStatus)
                    .entity(projectList)
                    .build();
        }
    }

    /**
     * reads a single project identified by the projectId
     *
     * @param projectUUID the projectUUID in the URL
     * @return Response
     */
    @GET
    @Path("read")
    @Produces(MediaType.APPLICATION_JSON)

    public Response readProject(
            @QueryParam("uuid") String projectUUID,
            @CookieParam("token") String token
    ) {
        Dao<Project, String> projectDAO = new ProjectDao();
        Project project = projectDAO.getEntity(projectUUID);
        if (project.getTitle() == null) {
            return Response
                    .status(404)
                    .entity("{\"error\":\"Kein Projekt gefunden\"}")
                    .build();
        } else {
            return Response
                    .status(200)
                    .entity(project)
                    .build();
        }
    }

    @POST
    @Path("save")
    @Produces(MediaType.TEXT_PLAIN)
    public Response saveProject(
            @FormParam("projectUUID") String projectUUID,
            @FormParam("title") String title,
            @FormParam("startDate") String startDate,
            @FormParam("endDate") String endDate,
            @FormParam("categoryUUID") String categoryUUID

            ) {
        int httpStatus;
        String message;
        Project project = new Project();
        project.setProjectUUID(projectUUID);
        project.setTitle(title);
        project.setStartDate(startDate);
        project.setEndDate(endDate);
        project.setCategory(new Category());
        project.getCategory().setCategoryUUID(categoryUUID);

        Dao<Project, String> projectDao = new ProjectDao();
        Result result = projectDao.save(project);
        if (result == Result.SUCCESS) {
            message = "Projekt gespeichert";
            httpStatus = 200;
        }else if (result == Result.DUPLICATE) {
            message = "Es existiert schon mit diesem Titel";
            httpStatus = 400;
        }
        else {
            message = "Fehler beim Speichern des Projekts";
            httpStatus = 500;
        }
        return Response
                .status(httpStatus)
                .entity(message)
                .build();
    }

    @DELETE
    @Path("delete")
    @Produces(MediaType.TEXT_PLAIN)
    public Response deleteProject(
            @QueryParam("uuid") String projectUUID
    ) {
        int httpStatus;
        String message;
        Dao<Project, String> projectDao = new ProjectDao();
        Result result = projectDao.delete(projectUUID);
        if (result == Result.SUCCESS) {
            httpStatus = 200;
            message = "Projekt gelöscht";
        } else if (result == Result.NOACTION) {
            httpStatus = 200;
            message = "Kein Projekt gefunden";
        } else {
            httpStatus = 500;
            message = "Fehler";
        }
        return Response
                .status(httpStatus)
                .entity(message)
                .build();
    }
}
