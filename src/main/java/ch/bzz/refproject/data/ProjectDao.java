package ch.bzz.refproject.data;

import ch.bzz.refproject.model.Project;
import ch.bzz.refproject.util.Result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class ProjectDao implements Dao<Project, String>{
    /**
     * empty constructor
     */
    public ProjectDao() {}

    @Override
    public List<Project> getAll() {
        List<Project> projectList = new ArrayList<>();
        String sqlQuery =
                "SELECT projectUUID, categoryUUID, title, startDate, endDate," +
                        "      status" +
                        "  FROM RefProject.Project" +
                        " ORDER BY title";
        try {
            ResultSet resultSet = MySqlDB.sqlSelect(sqlQuery);
            while (resultSet.next()) {
                Project project = new Project();
                setValues(resultSet, project);
                projectList.add(project);
            }
        } catch (SQLException sqlEx) {
            MySqlDB.printSQLException(sqlEx);
            throw new RuntimeException();
        } finally {

            MySqlDB.sqlClose();
        }
        return projectList;

    }

    @Override
    public Project getEntity(String projectUUID) {
        Project project = new Project();

        String sqlQuery =
                "SELECT projectUUID, categoryUUID, title, startDate, endDate," +
                        "      status" +
                        "  FROM RefProject.Project" +
                        " WHERE projectUUID=?";
        Map<Integer, Object> values = new HashMap<>();
        values.put(1, projectUUID);
        try {
            ResultSet resultSet = MySqlDB.sqlSelect(sqlQuery, values);
            if (resultSet.next()) {
                setValues(resultSet, project);
            }

        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
            throw new RuntimeException();
        } finally {
            MySqlDB.sqlClose();
        }
        return project;

    }


    @Override
    public Result delete(String projectUUID) {
        String sqlQuery =
                "DELETE FROM Project" +
                        " WHERE projectUUID=?";
        Map<Integer, Object> values = new HashMap<>();
        values.put(1, projectUUID);

        try {
            return MySqlDB.sqlUpdate(sqlQuery, values);
        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
            throw new RuntimeException();
        }
    }



    @Override
    public Result save(Project project) {
        Map<Integer, Object> values = new HashMap<>();
        String sqlQuery;
        if (project.getProjectUUID() == null) {
            project.setProjectUUID(UUID.randomUUID().toString());
            sqlQuery = "INSERT INTO Project";
        } else {
            sqlQuery = "REPLACE Project";
        }
        sqlQuery += " SET projectUUID=?," +
                " title=?," +
                " categoryUUID=?," +
                " startDate=?," +
                " endDate=?," +
                " status=?";

        values.put(1, project.getProjectUUID());
        values.put(2, project.getTitle());
        values.put(3, project.getCategory().getCategoryUUID().toString());
        values.put(4, project.getStartDate().toString());
        values.put(5, project.getEndDate().toString());
        values.put(6,project.getStatus());

        try {
            return MySqlDB.sqlUpdate(sqlQuery, values);
        } catch (SQLException sqlEx) {
            String sqlState = sqlEx.getSQLState();
            if (sqlState.equals("23000")){
                return Result.DUPLICATE;
            }
            sqlEx.printStackTrace();
            throw new RuntimeException();
        }
    }


    private void setValues(ResultSet resultSet, Project project) {
    }

}
