package partition_new_method;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import rx.Observable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, IntWritable, Text> {

    private RTree<Integer, Geometry> rtree;
    private Map<Integer, Polygon> polygonMap = new HashMap<>();
    private Map<Integer, Integer> pointCountMap = new HashMap<>();

    private List<String> points = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        rtree = RTree.create();
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        rtree = RTree.create();
        polygonMap.clear();
        pointCountMap.clear();
        points.clear();

        for (Text value : values) {
            String line = value.toString();
            String[] parts = line.split(",", 2);
            if (parts.length != 2) {
                continue;
            }

            int id = Integer.parseInt(parts[0]);
            String wkt = parts[1];

            if (wkt.startsWith("POLYGON")) {
                Polygon polygon = parsePolygon(wkt);
                Polygon.BoundingBox mbr = polygon._boundingBox;
                rtree = rtree.add(id, Geometries.rectangle(mbr.xMin, mbr.yMin, mbr.xMax, mbr.yMax));
                polygonMap.put(id, polygon);
            } else if (wkt.startsWith("POINT")) {
                points.add(line);
            }
        }


        for (String pointLine : points) {
            String[] parts = pointLine.split(",", 2);
            Point point = parsePoint(parts[1]);
            Observable<Entry<Integer, Geometry>> results = rtree.search(Geometries.point(point.x, point.y))
                    .map(entry -> entry);

            for (Entry<Integer, Geometry> entry : results.toBlocking().toIterable()) {
                Integer polygonId = entry.value();
                Polygon polygon = polygonMap.get(polygonId);
                if (polygonId != null && polygon.contains(point)) {
                    pointCountMap.put(polygonId, pointCountMap.getOrDefault(polygonId, 0) + 1);
                }
            }
        }

        for (Map.Entry<Integer, Integer> entry : pointCountMap.entrySet()) {
            context.write(key, new Text("多边形ID: " + entry.getKey() + ", 包含点的数量: " + entry.getValue()));
        }

    }

    private Polygon parsePolygon(String wkt) {
        wkt = wkt.replace("POLYGON((", "").replace("))", "");
        String[] pointsStr = wkt.split(",");
        Polygon.Builder builder = Polygon.Builder();
        for (String ptStr : pointsStr) {
            String[] coords = ptStr.split(" ");
            double x = Double.parseDouble(coords[0]);
            double y = Double.parseDouble(coords[1]);
            builder.addVertex(new Point(x, y));
        }
        return builder.close().build();
    }

    private Point parsePoint(String wkt) {
        wkt = wkt.replace("POINT(", "").replace(")", "");
        String[] coords = wkt.split(" ");
        double x = Double.parseDouble(coords[0]);
        double y = Double.parseDouble(coords[1]);
        return new Point(x, y);
    }

}
