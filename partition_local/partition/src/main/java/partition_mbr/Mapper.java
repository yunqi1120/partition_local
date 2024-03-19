package partition_mbr;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.geometry.Rectangle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import rx.Observable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, Text> {

    private RTree<Integer, Geometry> rtree;

    //初始化，创建分区并添加到rtree中
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        rtree = RTree.create();

        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://namenode:8020"), conf);
        // 从配置中读取网格文件路径
        String gridFilePathStr = conf.get("gridFilePath");
        Path gridFilePath = new Path(gridFilePathStr);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(gridFilePath)));


        String line;
        while ((line = br.readLine()) != null) {
            String[] parts = line.split("\\s+");
            if (parts.length != 5) continue;
            int partitionIndex = Integer.parseInt(parts[0]);//网格索引
            //网格位置
            double lowerLeftX = Double.parseDouble(parts[1]);
            double lowerLeftY = Double.parseDouble(parts[2]);
            double upperRightX = Double.parseDouble(parts[3]);
            double upperRightY = Double.parseDouble(parts[4]);
            Rectangle rect = Geometries.rectangle(lowerLeftX, lowerLeftY, upperRightX, upperRightY);
            rtree = rtree.add(partitionIndex, rect);//添加网格到rtree中
        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] parts = line.split(",", 3);
        if (parts.length != 3) {
            return;
        }
        //逗号分割每行输入为三部分：编号，MBR坐标，原数据
        String id = parts[0];
        String mbr = parts[1];
        String wkt = parts[2];

        String[] mbrParts = mbr.split(" ", 4);
        if (mbrParts.length != 4) return;

        try {
            double xMin = Double.parseDouble(mbrParts[0]);
            double yMin = Double.parseDouble(mbrParts[1]);
            double xMax = Double.parseDouble(mbrParts[2]);
            double yMax = Double.parseDouble(mbrParts[3]);
            //转化成rtree可以处理的形式
            Rectangle rtreeGeometry = Geometries.rectangle(xMin, yMin, xMax, yMax);
            //比较mbr与rtree节点的重叠部分，确定分区
            Observable<Entry<Integer, Geometry>> results = rtree.search(rtreeGeometry);
            for (Entry<Integer, Geometry> entry : results.toBlocking().toIterable()) {
                context.write(new IntWritable(entry.value()), new Text(id + "," + wkt)); // 输出分区索引和原始WKT数据
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }
}
