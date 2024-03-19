package partition;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.geometry.Rectangle;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import rx.Observable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, IntWritable, Text> {

    private RTree<Integer, Geometry> rtree;
    private WKTReader wktReader = new WKTReader();
    private Map<Integer, Polygon> polygonMap = new HashMap<>();
    private Map<Integer, Integer> pointCountMap = new HashMap<>();

    private List<String> points = new ArrayList<>();//value只能遍历一次，用于将多边形存入rtree中，此处points用于存储点的坐标

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

        // 第一次遍历：处理多边形
        for (Text value : values) {
            String line = value.toString();
            String[] parts = line.split(",", 2);
            if (parts.length != 2) {
                continue;
            }

            int id = Integer.parseInt(parts[0]);
            String wkt = parts[1];

            if (wkt.startsWith("POLYGON")) {
                try {
                    Polygon polygon = (Polygon) wktReader.read(wkt);
                    /*
                    if (!polygon.isValid()) {//isvalid判断有无自交情况
                        context.write(new IntWritable(id), new Text("无效多边形，已跳过"));
                        continue; //跳过无效多边形
                    }
                     */

                    Envelope env = polygon.getEnvelopeInternal();
                    Rectangle mbr = Geometries.rectangle(env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY());
                    rtree = rtree.add(id, mbr); // 使用多边形ID作为RTree的值
                    polygonMap.put(id, polygon);

                } catch (ParseException e) {
                    e.printStackTrace();
                }
            } else if (wkt.startsWith("POINT")) {
                points.add(line);
            }
        }


        // 第二次遍历：处理点
        for (String pointLine : points) {
            String[] parts = pointLine.split(",", 2);
            if (parts.length != 2) {
                continue;
            }

            String pointWkt = parts[1];
            try {
                Point point = (Point) wktReader.read(pointWkt);
                Observable<Entry<Integer, Geometry>> results = rtree.search(Geometries.point(point.getX(), point.getY()))
                        .map(entry -> entry);

                for (Entry<Integer, Geometry> entry : results.toBlocking().toIterable()) {
                    Integer polygonId = entry.value(); // 直接从RTree获得多边形ID
                    Polygon polygon = polygonMap.get(polygonId);
                    if (polygonId != null && polygon.contains(point)) {
                    //if (polygonId != null ) {
                        pointCountMap.put(polygonId, pointCountMap.getOrDefault(polygonId, 0) + 1);
                    }
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        // 输出每个多边形和其包含的点的数量
        for (Map.Entry<Integer, Integer> entry : pointCountMap.entrySet()) {
            context.write(key, new Text("多边形ID: " + entry.getKey() + ", 包含点的数量: " + entry.getValue()));
        }
    }
}
