package es.hadoop.datamodels;

import com.google.gson.annotations.SerializedName;

/**
 * Created by sunhaoran on 2017/4/14.
 */
public class confinfo {
    String name;
    String title;
    String confname;
    String href;
    String confurl;
    @SerializedName("id")
    String cid;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getConfname() {
        return confname;
    }

    public void setConfname(String confname) {
        this.confname = confname;
    }

    public String getHref() {
        return href;
    }

    public void setHref(String href) {
        this.href = href;
    }

    public String getConfurl() {
        return confurl;
    }

    public void setConfurl(String confurl) {
        this.confurl = confurl;
    }

    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }
}
